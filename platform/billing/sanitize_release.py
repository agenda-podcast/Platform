\
"""
Sanitize billing-state CSV assets to enforce policy:
- Do not use "DENIED" anywhere in transactions (or other billing-state CSVs).
- Unsuccessful processes must use status="FAILED".

This module downloads the billing-state-v1 Release assets, rewrites any "DENIED"
tokens found in CSV fields, and uploads the corrected assets back to the same
Release (clobber via DELETE+upload).

Stdlib-only.
"""
from __future__ import annotations

import csv
import json
import mimetypes
import os
from pathlib import Path
import sys
import urllib.parse
import urllib.request
import urllib.error
from typing import Dict, List, Tuple, Optional

from platform.billing.manifest import write_manifest

RELEASE_TAG_DEFAULT = "billing-state-v1"

# If you add more billing-state files later, include them here so manifest covers them.
MANIFEST_FILES = [
    "tenants_credits.csv",
    "transactions.csv",
    "transaction_items.csv",
    "promotion_redemptions.csv",
    "cache_index.csv",
    "workorders_log.csv",
    "module_runs_log.csv",
    "github_releases_map.csv",
    "github_assets_map.csv",
]


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
        "User-Agent": "platform-billing-sanitizer",
    }


def _request(method: str, url: str, headers: Dict[str, str], body: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url=url, data=body, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.getcode(), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        raise RuntimeError(f"HTTP {method} {url} failed: {e}") from e


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


def _get_release_by_tag(repo: str, token: str, tag: str) -> dict:
    url = f"{_gh_api_base(repo)}/releases/tags/{urllib.parse.quote(tag)}"
    code, data, txt = _json("GET", url, _api_headers(token))
    if code == 404:
        raise RuntimeError(f"Release tag not found: {tag}")
    if code >= 300 or data is None:
        raise RuntimeError(f"GET {url} failed: HTTP {code} {txt[:800]}")
    return data


def _normalize_upload_url(upload_url_template: str) -> str:
    return upload_url_template.split("{", 1)[0]


def _guess_content_type(path: Path) -> str:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=120) as resp:
        dest.write_bytes(resp.read())


def _delete_asset(repo: str, token: str, asset_id: int) -> None:
    url = f"{_gh_api_base(repo)}/releases/assets/{asset_id}"
    code, raw = _request("DELETE", url, _api_headers(token))
    if code not in (204, 404):
        txt = raw.decode("utf-8", errors="replace") if raw else ""
        raise RuntimeError(f"DELETE {url} failed: HTTP {code} {txt[:800]}")


def _upload_asset(upload_url_template: str, token: str, file_path: Path, name: str) -> None:
    upload_base = _normalize_upload_url(upload_url_template)
    url = f"{upload_base}?name={urllib.parse.quote(name)}"

    data = file_path.read_bytes()
    headers = _api_headers(token)
    headers["Content-Type"] = _guess_content_type(file_path)

    code, raw = _request("POST", url, headers, data)
    if code != 201:
        txt = raw.decode("utf-8", errors="replace") if raw else ""
        raise RuntimeError(f"POST upload {name} failed: HTTP {code} {txt[:800]}")


def _sanitize_csv_inplace(path: Path) -> bool:
    """
    Replace any literal cell value 'DENIED' with policy-compliant values.
    - If column is 'status' -> 'FAILED'
    - If column is 'type'   -> 'RUN' (and if status empty, set FAILED)
    - Otherwise -> 'FAILED'
    Returns True if file changed.
    """
    if not path.exists():
        return False

    original = path.read_text(encoding="utf-8", errors="replace")
    rows_out: List[List[str]] = []

    changed = False
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return False

        col_idx = {name: i for i, name in enumerate(header)}
        rows_out.append(header)

        for row in reader:
            # pad row to header length
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            row2 = list(row)

            for i, val in enumerate(row2):
                if val == "DENIED":
                    col_name = header[i] if i < len(header) else ""
                    if col_name == "status":
                        row2[i] = "FAILED"
                    elif col_name == "type":
                        row2[i] = "RUN"
                        # also ensure status FAILED if present and empty/DENIED
                        si = col_idx.get("status")
                        if si is not None:
                            if row2[si] in ("", "DENIED"):
                                row2[si] = "FAILED"
                    else:
                        row2[i] = "FAILED"
                    changed = True

            rows_out.append(row2)

    if not changed:
        return False

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows_out)

    # ensure file actually changed (guard)
    return path.read_text(encoding="utf-8", errors="replace") != original


def main() -> int:
    token = _env("GITHUB_TOKEN")
    repo = _env("GITHUB_REPOSITORY")
    tag = _env("BILLING_TAG", RELEASE_TAG_DEFAULT)

    workdir = Path(_env("BILLING_SANITIZE_DIR", ".billing-state-sanitize"))
    workdir.mkdir(parents=True, exist_ok=True)

    print(f"[billing-sanitize] repo={repo} tag={tag} workdir={workdir}")

    release = _get_release_by_tag(repo, token, tag)
    assets = release.get("assets") or []
    upload_url_template = release.get("upload_url")
    if not upload_url_template:
        raise RuntimeError("Release JSON missing upload_url")

    # Map asset name -> id, url
    by_name: Dict[str, dict] = {a.get("name"): a for a in assets if a.get("name")}

    # Download target CSVs if present
    changed_assets: List[str] = []
    for name, a in by_name.items():
        if not name.endswith(".csv"):
            continue
        dest = workdir / name
        _download(a["browser_download_url"], dest)
        if _sanitize_csv_inplace(dest):
            changed_assets.append(name)

    # Always (re)generate state_manifest.json based on possibly sanitized CSVs
    manifest_path = write_manifest(workdir, MANIFEST_FILES)
    if "state_manifest.json" not in by_name:
        changed_assets.append("state_manifest.json")
    else:
        # Compare downloaded vs regenerated, if different then treat as changed
        existing_manifest = workdir / "state_manifest.json.downloaded"
        _download(by_name["state_manifest.json"]["browser_download_url"], existing_manifest)
        if existing_manifest.read_bytes() != manifest_path.read_bytes():
            changed_assets.append("state_manifest.json")

    if not changed_assets:
        print("[billing-sanitize] No DENIED tokens found; manifest up-to-date. Nothing to do.")
        return 0

    # Upload changed assets: delete existing (if any) then upload
    print(f"[billing-sanitize] Updating assets: {sorted(set(changed_assets))}")
    for name in sorted(set(changed_assets)):
        src = workdir / name
        if not src.exists():
            # manifest file is named state_manifest.json in workdir by write_manifest
            if name == "state_manifest.json" and (workdir / "state_manifest.json").exists():
                src = workdir / "state_manifest.json"
            else:
                raise FileNotFoundError(f"Sanitized asset not found: {src}")

        existing = by_name.get(name)
        if existing and isinstance(existing.get("id"), int):
            _delete_asset(repo, token, int(existing["id"]))

        _upload_asset(upload_url_template, token, src, name)
        print(f"[billing-sanitize] Uploaded: {name}")

    print("[billing-sanitize] Done.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(str(e))
        sys.exit(2)
