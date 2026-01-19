"""
Publish default billing-state assets to the fixed GitHub Release tag (billing-state-v1)
if that release (or its required CSV assets) are missing. Existing assets are never overwritten.

- Idempotent: safe to run repeatedly.
- No external dependencies: uses Python stdlib only.
- Fails loudly if token/permissions are insufficient.

Required env (GitHub Actions provides GITHUB_REPOSITORY automatically):
- GITHUB_TOKEN
- GITHUB_REPOSITORY (owner/repo)

Optional env:
- BILLING_TAG (default billing-state-v1)
- BILLING_TEMPLATE_DIR (default releases/billing-state-v1)
"""
from __future__ import annotations

import json
import hashlib
import mimetypes
import os
import pathlib
import sys
import time
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List


REQUIRED_FILES = [
    "tenants_credits.csv",
    "transactions.csv",
    "transaction_items.csv",
    "promotion_redemptions.csv",
    "cache_index.csv",
            "github_releases_map.csv",
    "github_assets_map.csv",
    "state_manifest.json",
]


class TransientGitHubApiError(RuntimeError):
    """Transient GitHub API connectivity issue.

    We treat these as soft-fail conditions during Maintenance so the repository
    can still bootstrap local billing-state from the checked-in templates.
    """



def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        if default is None:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    return v.strip()


def _api_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "platform-maintenance-billing-bootstrap",
    }


def _is_transient_network_error(exc: Exception) -> bool:
    """Classify transient network errors.

    GitHub Actions runners occasionally hit transient DNS/TCP timeouts.
    Maintenance should not fail hard *before* it can bootstrap local
    billing-state from the checked-in templates.
    """

    # urllib wraps many socket failures in URLError.
    if isinstance(exc, urllib.error.URLError):
        r = getattr(exc, "reason", None)
        if isinstance(r, (TimeoutError, socket.timeout)):
            return True
        # Common string reasons
        rs = str(r).lower() if r is not None else str(exc).lower()
        if "timed out" in rs or "timeout" in rs:
            return True
        if "temporary failure" in rs or "name resolution" in rs:
            return True

    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True

    msg = str(exc).lower()
    if "timed out" in msg or "timeout" in msg:
        return True
    return False


def _request(method: str, url: str, headers: Dict[str, str], body: bytes | None = None) -> tuple[int, bytes]:
    """HTTP request with small, bounded retry for transient network errors.

    Rationale: Maintenance must remain reliable even if GitHub API has brief
    connectivity issues. If we still cannot reach GitHub after retries,
    callers may choose to soft-fail (skip remote ensure) when safe.
    """
    timeout_s = int(os.getenv("GITHUB_API_TIMEOUT", "30"))
    retries = int(os.getenv("GITHUB_API_RETRIES", "3"))
    backoff_s = float(os.getenv("GITHUB_API_RETRY_BACKOFF", "2"))

    req = urllib.request.Request(url=url, data=body, method=method)
    for k, v in headers.items():
        req.add_header(k, v)

    last_exc: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return resp.getcode(), resp.read()
        except urllib.error.HTTPError as e:
            # HTTPError is a response; do not retry here. Upstream logic will decide.
            return e.code, e.read()
        except Exception as e:
            last_exc = e
            if _is_transient_network_error(e) and attempt < retries:
                sleep_s = min(10.0, backoff_s ** (attempt - 1))
                print(f"[billing-bootstrap] Transient network error (attempt {attempt}/{retries}) {method} {url}: {e}. Retrying in {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            if _is_transient_network_error(e):
                raise TransientGitHubApiError(f"HTTP {method} {url} failed: {e}") from e
            raise RuntimeError(f"HTTP {method} {url} failed: {e}") from e

    # Should be unreachable, but keep a safe fallback.
    if last_exc is not None:
        if _is_transient_network_error(last_exc):
            raise TransientGitHubApiError(f"HTTP {method} {url} failed: {last_exc}") from last_exc
        raise RuntimeError(f"HTTP {method} {url} failed: {last_exc}") from last_exc
    raise RuntimeError(f"HTTP {method} {url} failed: unknown error")


def _json(method: str, url: str, headers: Dict[str, str], payload: dict | None = None) -> tuple[int, dict | None, str]:
    body = None
    h = dict(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        h["Content-Type"] = "application/json"
    code, raw = _request(method, url, h, body)
    txt = raw.decode("utf-8", errors="replace") if raw else ""
    if not txt:
        return code, None, ""
    try:
        return code, json.loads(txt), txt
    except Exception:
        return code, None, txt


def _gh_api_base(repo: str) -> str:
    return f"https://api.github.com/repos/{repo}"


def _get_release_by_tag(repo: str, token: str, tag: str) -> dict | None:
    url = f"{_gh_api_base(repo)}/releases/tags/{urllib.parse.quote(tag)}"
    code, data, txt = _json("GET", url, _api_headers(token))
    if code == 404:
        return None
    if code >= 300 or data is None:
        raise RuntimeError(f"GET {url} failed: HTTP {code} {txt[:800]}")
    return data


def _create_release(repo: str, token: str, tag: str) -> dict:
    url = f"{_gh_api_base(repo)}/releases"
    payload = {
        "tag_name": tag,
        "name": tag,
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
        "body": "Auto-published by Maintenance bootstrap (default billing-state template).",
    }
    code, data, txt = _json("POST", url, _api_headers(token), payload)
    if code >= 300 or data is None:
        raise RuntimeError(f"POST {url} failed: HTTP {code} {txt[:800]}")
    return data


def _normalize_upload_url(upload_url_template: str) -> str:
    # https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}
    return upload_url_template.split("{", 1)[0]


def _guess_content_type(path: pathlib.Path) -> str:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def _summarize_assets(release_json: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for a in release_json.get("assets", []) or []:
        n = a.get("name")
        i = a.get("id")
        if n and isinstance(i, int):
            out[n] = i
    return out


def _map_assets(release_json: dict) -> Dict[str, dict]:
    """Return name -> {id:int, url:str} from GitHub release JSON."""
    out: Dict[str, dict] = {}
    for a in release_json.get("assets", []) or []:
        n = a.get("name")
        i = a.get("id")
        u = a.get("url")  # API url: /releases/assets/{id}
        if n and isinstance(i, int) and isinstance(u, str) and u:
            out[n] = {"id": i, "url": u}
    return out


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _download_release_asset_bytes(asset_api_url: str, token: str) -> bytes:
    # GitHub API: GET /repos/{owner}/{repo}/releases/assets/{asset_id}
    # with Accept: application/octet-stream returns the raw bytes.
    headers = _api_headers(token)
    headers["Accept"] = "application/octet-stream"
    code, raw = _request("GET", asset_api_url, headers, None)
    if code >= 300:
        txt = raw.decode("utf-8", errors="replace") if raw else ""
        raise RuntimeError(f"[billing-bootstrap] Download asset failed: HTTP {code} {asset_api_url} {txt[:400]}")
    return raw


def _delete_release_asset(asset_api_url: str, token: str) -> None:
    headers = _api_headers(token)
    code, raw = _request("DELETE", asset_api_url, headers, None)
    if code not in (204, 404):
        txt = raw.decode("utf-8", errors="replace") if raw else ""
        raise RuntimeError(f"[billing-bootstrap] Delete asset failed: HTTP {code} {asset_api_url} {txt[:400]}")


def _upload_asset(upload_url_template: str, token: str, file_path: pathlib.Path, name: str) -> tuple[str, int, str]:
    upload_base = _normalize_upload_url(upload_url_template)
    url = f"{upload_base}?name={urllib.parse.quote(name)}"

    data = file_path.read_bytes()
    headers = _api_headers(token)
    headers["Content-Type"] = _guess_content_type(file_path)

    code, raw = _request("POST", url, headers, data)
    txt = raw.decode("utf-8", errors="replace") if raw else ""

    if code == 201:
        return ("ok", code, "")
    if code == 422:
        # asset exists or conflict
        return ("exists_or_conflict", code, txt[:800])
    return ("error", code, txt[:800])


def main() -> int:
    token = _env("GITHUB_TOKEN")
    repo = _env("GITHUB_REPOSITORY")
    tag = _env("BILLING_TAG", "billing-state-v1")
    template_dir = pathlib.Path(_env("BILLING_TEMPLATE_DIR", "releases/billing-state-v1"))

    print(f"[billing-bootstrap] repo={repo} tag={tag} template_dir={template_dir}")

    # Validate template files exist in repo checkout
    missing_local = [fn for fn in REQUIRED_FILES if not (template_dir / fn).exists()]
    if missing_local:
        raise FileNotFoundError(f"[billing-bootstrap] Missing template files in repo at {template_dir}: {missing_local}")

    try:
        release = _get_release_by_tag(repo, token, tag)
    except TransientGitHubApiError as e:
        # Critical: Maintenance must still be able to run offline against the
        # checked-in billing-state template. If GitHub API is temporarily
        # unreachable, skip remote release ensure and allow Maintenance to
        # bootstrap local billing-state from repo templates.
        print(f"[billing-bootstrap] Warning: GitHub API unreachable; skipping remote billing-state ensure for tag={tag}. {e}")
        return 0
    if release is None:
        print(f"[billing-bootstrap] Release tag not found: {tag}. Creating...")
        release = _create_release(repo, token, tag)
        print(f"[billing-bootstrap] Created release id={release.get('id')} url={release.get('html_url')}")
    else:
        print(f"[billing-bootstrap] Found release id={release.get('id')} url={release.get('html_url')}")

    upload_url_template = release.get("upload_url")
    if not upload_url_template:
        raise RuntimeError("[billing-bootstrap] Release JSON missing upload_url")
    # IMPORTANT: The Billing Release is the accounting Source of Truth.
    # Maintenance must NEVER overwrite existing assets in the Release with
    # repository templates. Templates are only used to seed *missing* assets
    # on first bootstrap (or if a file was never uploaded).

    existing_meta = _map_assets(release)
    missing = [fn for fn in REQUIRED_FILES if fn not in existing_meta]

    if not missing:
        print("[billing-bootstrap] All required assets already present. Nothing to do.")
        return 0

    print(f"[billing-bootstrap] Missing assets (will seed from template): {missing}")

    errors: List[str] = []

    # Upload missing assets only (never replace existing assets).
    for fn in missing:
        p = template_dir / fn
        status, code, text = _upload_asset(upload_url_template, token, p, fn)
        if status == "ok":
            print(f"[billing-bootstrap] Uploaded: {fn}")
        elif status == "exists_or_conflict":
            # Should not happen for missing, but tolerate a race.
            print(f"[billing-bootstrap] Asset exists/conflict (422): {fn}. Continuing.")
        else:
            errors.append(f"upload {fn}: HTTP {code} {text}")

    # Re-check release to confirm
    release2 = _get_release_by_tag(repo, token, tag)
    if release2 is None:
        errors.append("Release missing after creation (unexpected).")
    else:
        existing2 = _summarize_assets(release2)
        still_missing = [fn for fn in REQUIRED_FILES if fn not in existing2]
        if still_missing:
            errors.append(f"Still missing after upload attempts: {still_missing}")

    if errors:
        raise RuntimeError("[billing-bootstrap] Failed:\n- " + "\n- ".join(errors))

    print("[billing-bootstrap] Success: billing-state assets ensured.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(2)
