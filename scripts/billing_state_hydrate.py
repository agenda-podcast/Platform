#!/usr/bin/env python3
"""
Hydrate a local billing-state directory for an orchestrator run.

Policy:
- Prefer GitHub Release assets (billing-state-v1) when available.
- Fall back per-file to repository scaffold (fresh-start seed), typically .billing-state-ci.
- Fail only if a required file exists in neither place.
- Never copy Release assets back into the repository scaffold.
- Never fabricate placeholder/empty billing-state CSVs.

This script is designed to run in CI (GitHub Actions) and locally.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_REQUIRED_FILES = [
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

DEFAULT_RELEASE_TAG = "billing-state-v1"


def _which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)


def _truthy_env(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _repo_root_from_script() -> Path:
    # scripts/.. = repo root
    return Path(__file__).resolve().parents[1]


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_required_files() -> List[str]:
    """
    Prefer the canonical list from platform.billing.state if available,
    otherwise fall back to DEFAULT_REQUIRED_FILES.
    """
    try:
        from platform.billing import state as billing_state  # type: ignore

        if hasattr(billing_state, "REQUIRED_FILES"):
            rf = getattr(billing_state, "REQUIRED_FILES")
            if isinstance(rf, (list, tuple)) and all(isinstance(x, str) for x in rf):
                return list(rf)
    except Exception:
        pass
    return list(DEFAULT_REQUIRED_FILES)


def _list_release_assets(repo: str, tag: str) -> Set[str]:
    """
    Returns asset names for a GitHub release tag using gh CLI.
    If gh not available or release not found, returns empty set.
    """
    gh = _which("gh")
    if not gh:
        return set()

    cmd = [
        gh,
        "release",
        "view",
        tag,
        "--repo",
        repo,
        "--json",
        "assets",
        "--jq",
        ".assets[].name",
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        return set()
    assets = set()
    for line in (p.stdout or "").splitlines():
        name = line.strip()
        if name:
            assets.add(name)
    return assets


def _download_release_asset(repo: str, tag: str, asset_name: str, out_dir: Path) -> bool:
    """
    Attempts to download a single asset by name into out_dir using gh.
    Returns True if the file appears in out_dir after download.

    Notes:
    - We intentionally clobber local files when the asset exists in the Release.
      The Release is the Source of Truth; local billing-state may be a stale scaffold.
    """
    gh = _which("gh")
    if not gh:
        return False

    _safe_mkdir(out_dir)

    # gh release download supports --pattern; use exact name to avoid glob surprises.
    # Prefer --clobber to overwrite any local scaffold copy. If the installed gh doesn't
    # support --clobber, retry by deleting the destination file first.
    base_cmd = [
        gh,
        "release",
        "download",
        tag,
        "--repo",
        repo,
        "--dir",
        str(out_dir),
        "--pattern",
        asset_name,
    ]

    dst = out_dir / asset_name

    # First attempt: --clobber (modern gh)
    cmd = base_cmd + ["--clobber"]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode == 0 and dst.exists():
        return True

    # Retry: remove existing dst then download without --clobber (older gh)
    try:
        if dst.exists():
            dst.unlink()
    except Exception:
        pass

    p2 = subprocess.run(base_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p2.returncode != 0:
        return False
    return dst.exists()


def _copy_missing_from_scaffold(missing: Sequence[str], scaffold_dir: Path, target_dir: Path) -> List[str]:
    """
    Copies missing files from scaffold_dir into target_dir if present.
    Returns list of still-missing files after copy attempt.
    """
    still_missing: List[str] = []
    for name in missing:
        src = scaffold_dir / name
        dst = target_dir / name
        if src.exists() and src.is_file():
            _safe_mkdir(dst.parent)
            shutil.copyfile(src, dst)
        if not dst.exists():
            still_missing.append(name)
    return still_missing


def hydrate_billing_state_dir(
    billing_state_dir: Path,
    *,
    scaffold_dir: Optional[Path],
    release_tag: str,
    repo: Optional[str],
    required_files: Sequence[str],
    allow_release_download: bool,
) -> None:
    """
    Main hydration routine.

    1) Best-effort download of available required files from Release.
    2) Per-file fallback to scaffold for anything still missing.
    3) Fail if any required files still missing.
    """
    _safe_mkdir(billing_state_dir)

    # Determine repo
    if repo is None:
        repo = (os.getenv("GITHUB_REPOSITORY") or "").strip() or None

    # Step 1: release best-effort
    if allow_release_download and repo and _which("gh") and (os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")):
        assets = _list_release_assets(repo, release_tag)
        # Attempt to download any missing required files that exist as assets
        for name in required_files:
            # Always prefer Release assets when present; clobber local scaffold copies.
            if name in assets:
                _download_release_asset(repo, release_tag, name, billing_state_dir)

    # Step 2: scaffold fallback per-file
    missing = [n for n in required_files if not (billing_state_dir / n).exists()]
    if missing and scaffold_dir:
        missing = _copy_missing_from_scaffold(missing, scaffold_dir, billing_state_dir)

    # Step 3: fail only if not present anywhere
    if missing:
        raise FileNotFoundError(
            "Billing-state is missing required files after hydration: "
            + ", ".join(missing)
            + ".\n"
            + "Checked: (a) GitHub Release assets (best-effort) and (b) repo scaffold directory.\n"
            + f"billing_state_dir={billing_state_dir}\n"
            + f"scaffold_dir={scaffold_dir}\n"
            + f"release_tag={release_tag}\n"
            + f"repo={repo}\n"
        )


def _default_scaffold_dir(repo_root: Path) -> Optional[Path]:
    # Preferred scaffold seed
    p = repo_root / ".billing-state-ci"
    if p.exists() and p.is_dir():
        return p
    # Secondary: historical template location (if present)
    q = repo_root / "releases" / DEFAULT_RELEASE_TAG
    if q.exists() and q.is_dir():
        return q
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True, help="Target billing-state dir used by orchestrator")
    ap.add_argument("--scaffold-dir", default="", help="Repo scaffold directory to seed missing files (optional)")
    ap.add_argument("--release-tag", default=DEFAULT_RELEASE_TAG, help="Billing-state release tag")
    ap.add_argument("--repo", default="", help="owner/repo (defaults to GITHUB_REPOSITORY)")
    ap.add_argument(
        "--no-release-download",
        action="store_true",
        help="Disable attempting to download billing-state assets from GitHub Releases",
    )
    args = ap.parse_args()

    repo_root = _repo_root_from_script()
    billing_state_dir = Path(args.billing_state_dir).resolve()
    scaffold_dir = Path(args.scaffold_dir).resolve() if args.scaffold_dir.strip() else _default_scaffold_dir(repo_root)
    required = _read_required_files()

    try:
        hydrate_billing_state_dir(
            billing_state_dir,
            scaffold_dir=scaffold_dir,
            release_tag=args.release_tag,
            repo=args.repo.strip() or None,
            required_files=required,
            allow_release_download=(not args.no_release_download),
        )
    except FileNotFoundError as e:
        print(str(e))
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
