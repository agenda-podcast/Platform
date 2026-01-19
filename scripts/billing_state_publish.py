#!/usr/bin/env python3
"""Publish local billing-state CSV assets to the fixed GitHub Release tag.

Design goals:
  - Release tag is fixed (default: billing-state-v1).
  - Never writes billing-state files back into the repository.
  - Best-effort offline behavior: if GitHub env/token missing, no-op.
  - Deterministic: deletes existing assets with same name then uploads.

Expected environment on GitHub Actions:
  - GITHUB_TOKEN or GH_TOKEN
  - GITHUB_REPOSITORY = "owner/repo"
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, List
import json
import hashlib


def _env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _requests():
    import requests  # type: ignore

    return requests


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "platform-billing-state-publish",
    }


def _ensure_release(repo: str, token: str, tag: str) -> Dict[str, Any]:
    req = _requests()
    base = f"https://api.github.com/repos/{repo}"

    r = req.get(f"{base}/releases/tags/{tag}", headers=_headers(token), timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code != 404:
        raise RuntimeError(f"GitHub API error fetching release by tag: {r.status_code}: {r.text[:2000]}")

    payload = {
        "tag_name": tag,
        "name": tag,
        "body": "PLATFORM billing-state source of truth (auto-updated by orchestrator).",
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
    }
    r2 = req.post(f"{base}/releases", headers=_headers(token), json=payload, timeout=30)
    if r2.status_code != 201:
        raise RuntimeError(f"GitHub API error creating release: {r2.status_code}: {r2.text[:2000]}")
    return r2.json()


def _delete_asset_if_exists(repo: str, token: str, release: Dict[str, Any], name: str) -> None:
    req = _requests()
    base = f"https://api.github.com/repos/{repo}"
    for a in (release.get("assets") or []):
        if str(a.get("name")) != name:
            continue
        asset_id = a.get("id")
        if asset_id is None:
            continue
        r = req.delete(f"{base}/releases/assets/{asset_id}", headers=_headers(token), timeout=30)
        if r.status_code not in (204, 404):
            raise RuntimeError(f"GitHub API error deleting asset '{name}': {r.status_code}: {r.text[:2000]}")


def _upload_asset(repo: str, token: str, release: Dict[str, Any], path: Path) -> None:
    req = _requests()

    upload_url = str(release.get("upload_url") or "").split("{")[0]
    if not upload_url:
        raise RuntimeError("GitHub release payload missing upload_url")

    name = path.name
    content = path.read_bytes()

    headers = _headers(token)
    headers["Content-Type"] = "application/octet-stream"

    r = req.post(f"{upload_url}?name={name}", headers=headers, data=content, timeout=120)
    if r.status_code != 201:
        raise RuntimeError(f"GitHub API error uploading asset '{name}': {r.status_code}: {r.text[:2000]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--release-tag", default="billing-state-v1")
    ap.add_argument("--repo", default="")
    args = ap.parse_args()

    billing_dir = Path(args.billing_state_dir)
    tag = str(args.release_tag).strip() or "billing-state-v1"

    repo = (args.repo or _env("GITHUB_REPOSITORY")).strip()
    token = (_env("GH_TOKEN") or _env("GITHUB_TOKEN")).strip()

    # Offline-safe behavior
    if not repo or not token:
        print("[BILLING_PUBLISH][SKIP] missing repo/token")
        return 0

    required = [
        "tenants_credits.csv",
        "transactions.csv",
        "transaction_items.csv",
        "promotion_redemptions.csv",
        "cache_index.csv",
                        "github_releases_map.csv",
        "github_assets_map.csv",
        "state_manifest.json",
    ]

    baseline_path = billing_dir / "baseline_manifest.json"
    in_ci = (os.getenv("GITHUB_ACTIONS") or "").strip().lower() in ("1", "true", "yes")
    if in_ci and not baseline_path.exists():
        print("[BILLING_PUBLISH][SKIP] missing baseline_manifest.json (refusing to overwrite SoT from scaffold)")
        return 0

    baseline: Dict[str, str] = {}
    if baseline_path.exists():
        try:
            data = json.loads(baseline_path.read_text(encoding="utf-8"))
            for a in (data.get("assets") or []):
                n = str(a.get("name") or "").strip()
                h = str(a.get("sha256") or "").strip()
                if n and h:
                    baseline[n] = h
        except Exception:
            baseline = {}

    changed = False
    for fname in required:
        p = billing_dir / fname
        if not p.exists():
            continue
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        if baseline.get(fname) != h:
            changed = True
            break

    if not changed:
        print("[BILLING_PUBLISH][SKIP] no billing-state changes detected")
        return 0

    missing = [f for f in required if not (billing_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"Billing publish missing local files: {missing}")

    release = _ensure_release(repo, token, tag)

    # Refresh assets list after create/fetch
    # (API returns assets list, but we may need the latest)
    req = _requests()
    base = f"https://api.github.com/repos/{repo}"
    r = req.get(f"{base}/releases/tags/{tag}", headers=_headers(token), timeout=30)
    if r.status_code == 200:
        release = r.json()

    for fname in required:
        p = billing_dir / fname
        _delete_asset_if_exists(repo, token, release, p.name)

    # Re-fetch to avoid trying to delete twice if the API response was stale
    r = req.get(f"{base}/releases/tags/{tag}", headers=_headers(token), timeout=30)
    if r.status_code == 200:
        release = r.json()

    for fname in required:
        _upload_asset(repo, token, release, billing_dir / fname)

    print(f"[BILLING_PUBLISH][OK] published {len(required)} assets to tag={tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
