\
"""
Publish default billing-state assets to the fixed GitHub Release tag (billing-state-v1)
if that release (or its required CSV assets) are missing.

Design goals:
- Idempotent: safe to run repeatedly.
- No gh CLI dependency. Uses GitHub REST API.
- Fails loudly with actionable logs if token/permissions are insufficient.

Env:
- GITHUB_TOKEN (required)
- GITHUB_REPOSITORY (owner/repo) provided by Actions
- BILLING_TAG (optional, default billing-state-v1)
- BILLING_TEMPLATE_DIR (optional, default releases/billing-state-v1)
"""
from __future__ import annotations

import json
import mimetypes
import os
import pathlib
import sys
import time
from typing import Dict, List, Tuple

import requests


REQUIRED_FILES = [
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
        "User-Agent": "platform-maintenance-billing-bootstrap",
    }


def _gh_api_base(repo: str) -> str:
    return f"https://api.github.com/repos/{repo}"


def _request_json(method: str, url: str, headers: Dict[str, str], **kwargs):
    r = requests.request(method, url, headers=headers, timeout=60, **kwargs)
    return r


def _get_release_by_tag(repo: str, token: str, tag: str):
    url = f"{_gh_api_base(repo)}/releases/tags/{tag}"
    r = _request_json("GET", url, _api_headers(token))
    if r.status_code == 404:
        return None
    if r.status_code >= 300:
        raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text}")
    return r.json()


def _create_release(repo: str, token: str, tag: str):
    url = f"{_gh_api_base(repo)}/releases"
    payload = {
        "tag_name": tag,
        "name": tag,
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
        "body": "Auto-published by Maintenance bootstrap (default billing-state template).",
    }
    r = _request_json("POST", url, _api_headers(token), json=payload)
    if r.status_code >= 300:
        raise RuntimeError(f"POST {url} failed: {r.status_code} {r.text}")
    return r.json()


def _normalize_upload_url(upload_url_template: str) -> str:
    # Template looks like: https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}
    return upload_url_template.split("{", 1)[0]


def _guess_content_type(path: pathlib.Path) -> str:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def _upload_asset(upload_url_template: str, repo: str, token: str, file_path: pathlib.Path, name: str):
    upload_base = _normalize_upload_url(upload_url_template)
    url = f"{upload_base}?name={name}"
    data = file_path.read_bytes()
    headers = _api_headers(token)
    headers["Content-Type"] = _guess_content_type(file_path)
    r = requests.post(url, headers=headers, data=data, timeout=120)
    if r.status_code == 422:
        # asset exists or name conflict; treat as ok (we gate on missing names first)
        return ("exists_or_conflict", r.status_code, r.text)
    if r.status_code >= 300:
        return ("error", r.status_code, r.text)
    return ("ok", r.status_code, "")


def _summarize_assets(release_json) -> Dict[str, int]:
    # name -> id
    out = {}
    for a in release_json.get("assets", []):
        n = a.get("name")
        i = a.get("id")
        if n and i:
            out[n] = i
    return out


def main() -> int:
    token = _env("GITHUB_TOKEN")
    repo = _env("GITHUB_REPOSITORY")
    tag = _env("BILLING_TAG", "billing-state-v1")
    template_dir = pathlib.Path(_env("BILLING_TEMPLATE_DIR", "releases/billing-state-v1"))

    print(f"[billing-bootstrap] repo={repo} tag={tag} template_dir={template_dir}")

    # Validate template files exist in repo checkout
    missing_local = [fn for fn in REQUIRED_FILES if not (template_dir / fn).exists()]
    if missing_local:
        raise FileNotFoundError(
            f"[billing-bootstrap] Missing template files in repo at {template_dir}: {missing_local}"
        )

    release = _get_release_by_tag(repo, token, tag)
    if release is None:
        print(f"[billing-bootstrap] Release tag not found: {tag}. Creating...")
        release = _create_release(repo, token, tag)
        print(f"[billing-bootstrap] Created release id={release.get('id')} url={release.get('html_url')}")
    else:
        print(f"[billing-bootstrap] Found release id={release.get('id')} url={release.get('html_url')}")

    upload_url_template = release.get("upload_url")
    if not upload_url_template:
        raise RuntimeError("[billing-bootstrap] Release JSON missing upload_url")

    existing = _summarize_assets(release)
    to_upload = [fn for fn in REQUIRED_FILES if fn not in existing]

    if not to_upload:
        print("[billing-bootstrap] All required assets already present. Nothing to do.")
        return 0

    print(f"[billing-bootstrap] Missing assets: {to_upload}")
    errors: List[str] = []

    for fn in to_upload:
        p = template_dir / fn
        status, code, text = _upload_asset(upload_url_template, repo, token, p, fn)
        if status == "ok":
            print(f"[billing-bootstrap] Uploaded: {fn}")
        elif status == "exists_or_conflict":
            print(f"[billing-bootstrap] Asset already exists/conflict: {fn} (422). Continuing.")
        else:
            print(f"[billing-bootstrap] ERROR uploading {fn}: HTTP {code}")
            # Avoid dumping huge bodies
            errors.append(f"{fn}: HTTP {code} {text[:500]}")

    # Re-check release to confirm
    release2 = _get_release_by_tag(repo, token, tag)
    if release2 is None:
        errors.append("Release disappeared after creation (unexpected).")
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
