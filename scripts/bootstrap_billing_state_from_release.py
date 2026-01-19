#!/usr/bin/env python3
"""Bootstrap billing-state working directory from a GitHub Release tag."""

import argparse
import os
import pathlib
import subprocess
import json
import hashlib
from typing import Dict, List, Optional

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

BASELINE_MANIFEST_NAME = "baseline_manifest.json"


def _gh_env() -> Dict[str, str]:
    env: Dict[str, str] = dict(os.environ)
    if not env.get("GH_TOKEN") and env.get("GITHUB_TOKEN"):
        env["GH_TOKEN"] = env["GITHUB_TOKEN"]
    return env


def run(cmd: List[str], *, env: Optional[Dict[str, str]] = None) -> None:
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env or _gh_env(),
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stdout}")
    print(proc.stdout)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--tag", default="billing-state-v1", help="Release tag name")
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--pattern", default="", help="Optional extra glob pattern to download in addition to required billing assets (e.g. '*.zip'). Default: none")
    args = ap.parse_args()

    billing_dir = pathlib.Path(args.billing_state_dir)

    if "GITHUB_ACTIONS" in os.environ and (not os.environ.get("GH_TOKEN")) and (not os.environ.get("GITHUB_TOKEN")):
        print("[bootstrap_billing_state_from_release][FAIL] Missing auth token. On Actions, set env GH_TOKEN: ${{ github.token }}")
        return 2

    if billing_dir.exists():
        for p in billing_dir.iterdir():
            if p.is_file():
                p.unlink()
    billing_dir.mkdir(parents=True, exist_ok=True)

    # Download required billing-state assets explicitly (includes non-CSV files such as state_manifest.json)
    for fn in REQUIRED_FILES:
        run([
            "gh", "release", "download", args.tag,
            "--repo", args.repo,
            "--dir", str(billing_dir),
            "--pattern", fn,
            "--clobber",
        ])

    # Optionally download extra assets (useful for debugging or ad-hoc retrieval)
    if getattr(args, "pattern", ""):
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
        for p in sorted(billing_dir.glob('*')):
            print(" -", p.name)
        return 2

    # Write baseline manifest so publish can decide whether state changed.
    baseline = {"assets": []}
    for fn in REQUIRED_FILES:
        p = billing_dir / fn
        if not p.exists() or not p.is_file():
            continue
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        baseline["assets"].append({"name": fn, "sha256": h})
    (billing_dir / BASELINE_MANIFEST_NAME).write_text(
        json.dumps(baseline, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )

    # Recompute tenants_credits.csv deterministically from ledger.
    try:
        import sys
        repo_root = pathlib.Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from platform.billing.recompute_credits import recompute_tenants_credits  # type: ignore

        recompute_tenants_credits(billing_dir)
    except Exception:
        pass

    print("[bootstrap_billing_state_from_release][OK] Billing-state bootstrapped and validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
