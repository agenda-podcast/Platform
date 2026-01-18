"""
Ensure the fixed billing-state GitHub Release (tag: billing-state-v1) exists
and contains all required billing CSV assets.

Design goals:
- No dependency on gh CLI or marketplace actions for GitHub API calls.
- Works with GitHub Actions' GITHUB_TOKEN (needs contents: write).
- Idempotent: safe to run on every Maintenance execution.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests


BILLING_TAG = "billing-state-v1"
REQUIRED_FILES = [
    "tenants_credits.csv",
    "transactions.csv",
    "transaction_items.csv",
    "promotion_redemptions.csv",
    "cache_index.csv",
            "github_releases_map.csv",
    "github_assets_map.csv",
]


def _repo_slug() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not repo or "/" not in repo:
        raise RuntimeError("GITHUB_REPOSITORY is not set (expected 'owner/repo').")
    return repo


def _token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GITHUB_TOKEN is not set. In Actions, pass env: GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}.")
    return token


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "platform-maintenance-billing-bootstrap",
    }


def _repo_root() -> pathlib.Path:
    # This file lives at platform/billing/publish_default_release.py
    # Repo root is 3 levels up.
    return pathlib.Path(__file__).resolve().parents[2]


def _template_dir() -> pathlib.Path:
    return _repo_root() / "releases" / BILLING_TAG


def _load_template_bytes(filename: str) -> bytes:
    p = _template_dir() / filename
    if not p.exists():
        raise FileNotFoundError(f"Template billing file missing in repo: {p}")
    return p.read_bytes()


def _api_get(url: str, headers: Dict[str, str]) -> requests.Response:
    r = requests.get(url, headers=headers, timeout=60)
    return r


def _api_post(url: str, headers: Dict[str, str], payload: dict) -> requests.Response:
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    return r


def _api_delete(url: str, headers: Dict[str, str]) -> requests.Response:
    r = requests.delete(url, headers=headers, timeout=60)
    return r


def _upload_asset(upload_url_template: str, headers: Dict[str, str], name: str, data: bytes) -> None:
    # upload_url_template like: https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}
    base = upload_url_template.split("{", 1)[0]
    url = f"{base}?name={name}"
    up_headers = dict(headers)
    up_headers["Content-Type"] = "application/octet-stream"
    # GitHub can be flaky; retry a couple of times.
    last_err = None
    for _ in range(3):
        resp = requests.post(url, headers=up_headers, data=data, timeout=120)
        if resp.status_code in (200, 201):
            return
        last_err = (resp.status_code, resp.text[:1000])
        time.sleep(2)
    raise RuntimeError(f"Asset upload failed for {name}: {last_err}")


def ensure_billing_release_assets(tag: str = BILLING_TAG) -> None:
    repo = _repo_slug()
    token = _token()
    headers = _headers(token)

    # Validate template presence in repo
    tdir = _template_dir()
    if not tdir.exists():
        raise FileNotFoundError(
            f"Template billing release folder not found in repo: {tdir}. "
            f"Expected defaults at releases/{tag}/"
        )
    missing_local = [f for f in REQUIRED_FILES if not (tdir / f).exists()]
    if missing_local:
        raise FileNotFoundError(f"Repo template releases/{tag} is missing required files: {missing_local}")

    # 1) Try fetch release by tag
    get_url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    r = _api_get(get_url, headers)
    if r.status_code == 404:
        # 2) Create the release
        create_url = f"https://api.github.com/repos/{repo}/releases"
        payload = {
            "tag_name": tag,
            "name": tag,
            "body": "Fixed billing state release (source-of-truth CSV ledger). Managed by Maintenance.",
            "draft": False,
            "prerelease": False,
        }
        cr = _api_post(create_url, headers, payload)
        if cr.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create release {tag}: {cr.status_code} {cr.text[:1200]}")
        rel = cr.json()
    elif r.status_code == 200:
        rel = r.json()
    else:
        raise RuntimeError(f"Failed to read release by tag {tag}: {r.status_code} {r.text[:1200]}")

    release_id = rel["id"]
    upload_url = rel["upload_url"]

    # 3) Determine which assets exist
    assets = rel.get("assets", []) or []
    existing_names = {a.get("name") for a in assets if a.get("name")}
    # If assets list is truncated (rare), fetch assets via API
    if len(existing_names) < len(assets):
        existing_names = set()

    # 4) Upload missing assets
    to_upload = [f for f in REQUIRED_FILES if f not in existing_names]
    if not to_upload:
        print(f"[billing-bootstrap] Release '{tag}' already has all required assets.")
        return

    print(f"[billing-bootstrap] Ensuring release '{tag}' has required assets: {to_upload}")
    for fn in to_upload:
        data = _load_template_bytes(fn)
        _upload_asset(upload_url, headers, fn, data)
        print(f"[billing-bootstrap] Uploaded: {fn}")

    print(f"[billing-bootstrap] Release '{tag}' is now bootstrapped.")


def main(argv: List[str]) -> int:
    tag = BILLING_TAG
    if len(argv) >= 2 and argv[1].strip():
        tag = argv[1].strip()
    ensure_billing_release_assets(tag=tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
