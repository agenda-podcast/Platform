"""Implementation for release syncing.

Separated from scripts/release_sync.py so that release_sync.py can act as a thin,
import-safe wrapper for both module and legacy script invocation.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scripts.id_manager import IdManager, ReleaseIdMap

def _run(cmd: List[str], *, cwd: str | None = None) -> str:
    p = subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True)
    return p.stdout.strip()


def _gh_json(args: List[str]) -> Any:
    out = _run(["gh", *args])
    return json.loads(out) if out else None


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not repo or "/" not in repo:
        raise SystemExit("GITHUB_REPOSITORY env var not set (expected 'owner/repo')")

    os.makedirs("releases", exist_ok=True)
    idm = IdManager()
    relmap = ReleaseIdMap()

    # List releases (up to 100). For most repos, sufficient; can be extended with pagination.
    releases: List[Dict[str, Any]] = _gh_json(["api", f"repos/{repo}/releases?per_page=100"])

    for r in releases:
        gh_id = int(r["id"])
        tag = r.get("tag_name") or ""

        if tag == BILLING_STATE_TAG:
            folder = os.path.join("releases", BILLING_STATE_TAG)
        else:
            existing = relmap.get_by_github_release_id(gh_id)
            if existing:
                alias = existing["release_alias_id"]
            else:
                alias = idm.new_release_alias_id()
                relmap.add(alias, gh_id, tag)
            folder = os.path.join("releases", alias)

        os.makedirs(folder, exist_ok=True)
        meta_path = os.path.join(folder, "release_meta.json")
        meta = {
            "github_release_id": gh_id,
            "tag": tag,
            "name": r.get("name"),
            "draft": bool(r.get("draft")),
            "prerelease": bool(r.get("prerelease")),
            "published_at": r.get("published_at"),
            "synced_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "html_url": r.get("html_url"),
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        assets_dir = os.path.join(folder, "assets")
        os.makedirs(assets_dir, exist_ok=True)

        # Download assets by tag into this folder.
        # - Skip if no assets present to reduce noise/time.
        assets = r.get("assets") or []
        if assets:
            _run(["gh", "release", "download", tag, "--repo", repo, "--dir", assets_dir, "--clobber"])

    print("OK: Release sync completed.")

