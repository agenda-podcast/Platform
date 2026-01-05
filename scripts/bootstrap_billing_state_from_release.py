#!/usr/bin/env python3
"""Bootstrap billing-state working directory from a GitHub Release tag.

Rationale:
- Orchestrator requires a minimal set of billing-state CSVs to exist locally.
- Source of truth is the fixed Release tag (default: billing-state-v1).

This script downloads the Release assets into --billing-state-dir and validates required files exist.
"""

import argparse
import os
import pathlib
import subprocess
from typing import List

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


def run(cmd: List[str]) -> None:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    print(proc.stdout)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--tag", default="billing-state-v1", help="Release tag name")
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--pattern", default="*.csv", help="Asset glob pattern to download (default: *.csv)")
    args = ap.parse_args()

    billing_dir = pathlib.Path(args.billing_state_dir)

    if "GITHUB_ACTIONS" in os.environ and not os.environ.get("GH_TOKEN"):
        print("[bootstrap_billing_state_from_release][FAIL] GH_TOKEN is not set. On Actions, set env GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}")
        return 2

    # Clean target dir to prevent stale files
    if billing_dir.exists():
        for p in billing_dir.iterdir():
            if p.is_file():
                p.unlink()
    billing_dir.mkdir(parents=True, exist_ok=True)

    run([
        "gh", "release", "download", args.tag,
        "--repo", args.repo,
        "--dir", str(billing_dir),
        "--pattern", args.pattern,
        "--clobber",
    ])

    missing = [fn for fn in REQUIRED_FILES if not (billing_dir / fn).exists()]
    if missing:
        print(f"[bootstrap_billing_state_from_release][FAIL] Billing-state is missing required files after download: {missing}")
        print("[bootstrap_billing_state_from_release] Directory listing:")
        for p in sorted(billing_dir.glob("*")):
            print(" -", p.name)
        return 2

    print("[bootstrap_billing_state_from_release][OK] Billing-state bootstrapped and validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
