#!/usr/bin/env python3
"""CI/runtime guard for publishing module artifacts to GitHub Releases.

This project supports a paid option to make module-produced artifacts downloadable
from GitHub Releases. When a workorder requests this option, the run must have
GitHub auth available (GH_TOKEN or GITHUB_TOKEN) and (optionally) gh CLI.

This guard is intentionally lightweight and repository-local: it scans tenants/*/workorders/*.yml
and determines whether any module requests release artifacts.

Exit codes:
  0 = OK / not required
  2 = required but environment cannot satisfy (missing token / missing gh)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import yaml

REASON_KEY_TOKEN_MISSING = "artifacts_release_token_missing"
REASON_KEY_GHCLI_MISSING = "artifacts_release_ghcli_missing"


def _repo_root() -> Path:
    override = (os.getenv("PLATFORM_REPO_ROOT") or "").strip()
    if override:
        return Path(override).resolve()
    return Path(__file__).resolve().parents[1]


def _read_yaml(path: Path) -> Dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        # If YAML is malformed, let the orchestrator/ci_verify fail later.
        return {}


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _iter_workorders(repo_root: Path) -> Iterable[Path]:
    tenants = repo_root / "tenants"
    if not tenants.exists():
        return []
    for td in tenants.iterdir():
        if not td.is_dir():
            continue
        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in wdir.glob("*.yml"):
            yield wp


def _workorder_requests_release_artifacts(workorder: Dict[str, Any]) -> bool:
    # Support a few likely keys to avoid brittle coupling:
    # - module-level: purchase_release_artifacts: true
    # - module-level: artifacts_download: true
    # - module-level: artifacts_download_purchased: true
    # - workorder-level: purchase_release_artifacts: true (global default)

    if _truthy(workorder.get("purchase_release_artifacts")):
        return True

    modules = workorder.get("modules") or []
    if not isinstance(modules, list):
        return False

    for m in modules:
        if not isinstance(m, dict):
            continue
        if _truthy(m.get("purchase_release_artifacts")):
            return True
        if _truthy(m.get("artifacts_download")):
            return True
        if _truthy(m.get("artifacts_download_purchased")):
            return True
    return False


def needs_release_artifacts(repo_root: Path) -> bool:
    for wp in _iter_workorders(repo_root):
        wo = _read_yaml(wp)
        if _workorder_requests_release_artifacts(wo):
            return True
    return False


def _has_token() -> bool:
    return bool((os.getenv("GH_TOKEN") or "").strip() or (os.getenv("GITHUB_TOKEN") or "").strip())


def _has_gh_cli() -> bool:
    # Optional; some implementations use GitHub API directly.
    # If gh isn't used, this can be ignored by setting PLATFORM_RELEASES_NO_GHCLI=1.
    if (os.getenv("PLATFORM_RELEASES_NO_GHCLI") or "").strip().lower() in ("1", "true", "yes", "on"):
        return True

    from shutil import which

    return which("gh") is not None


def enforce(repo_root: Path) -> int:
    if not needs_release_artifacts(repo_root):
        return 0

    if not _has_token():
        print(
            "[ORCHESTRATOR][DENY] Release artifacts requested but no GitHub token available. "
            f"reason_key={REASON_KEY_TOKEN_MISSING}. "
            "Set GH_TOKEN or GITHUB_TOKEN (with repo release permissions).",
            file=sys.stderr,
        )
        return 2

    if not _has_gh_cli():
        print(
            "[ORCHESTRATOR][DENY] Release artifacts requested but GitHub CLI (gh) not found. "
            f"reason_key={REASON_KEY_GHCLI_MISSING}. "
            "Install gh or set PLATFORM_RELEASES_NO_GHCLI=1 if your implementation uses API calls.",
            file=sys.stderr,
        )
        return 2

    return 0


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--needs-releases-flag",
        action="store_true",
        help="Print 1 if any workorder requests release artifacts, else 0.",
    )
    ap.add_argument(
        "--enforce",
        action="store_true",
        help="Enforce environment requirements when release artifacts are requested.",
    )
    args = ap.parse_args(argv)

    repo_root = _repo_root()

    if args.needs_releases_flag:
        print("1" if needs_release_artifacts(repo_root) else "0")
        return 0

    # Default behavior is to enforce.
    if args.enforce or not args.needs_releases_flag:
        return enforce(repo_root)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
