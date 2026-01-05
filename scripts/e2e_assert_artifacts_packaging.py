#!/usr/bin/env python3
"""Offline E2E assertion for artifacts packaging.

Expected usage (in your existing E2E workflow after orchestrator run):
  python scripts/publish_artifacts_release.py --billing-state-dir ... --runtime-dir ... --since ... --repo owner/repo --no-publish
  python scripts/e2e_assert_artifacts_packaging.py --dist-dir dist_artifacts
"""

import argparse
import pathlib
import sys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist-dir", default="dist_artifacts")
    ap.add_argument("--min-zips", type=int, default=1)
    args = ap.parse_args()

    dist = pathlib.Path(args.dist_dir)
    zips = sorted(dist.glob("*.zip"))

    if len(zips) < args.min_zips:
        print(f"[E2E][FAIL] Expected at least {args.min_zips} ZIP(s) in {dist}, found {len(zips)}")
        if dist.exists():
            for p in dist.iterdir():
                print(" -", p)
        return 2

    print(f"[E2E][OK] Found {len(zips)} ZIP(s) in {dist}:")
    for z in zips[:10]:
        print(" -", z.name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
