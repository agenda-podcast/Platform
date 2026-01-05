#!/usr/bin/env python3
import argparse
import pathlib

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist-dir", default="dist_artifacts")
    ap.add_argument("--min-zips", type=int, default=1)
    args = ap.parse_args()

    dist = pathlib.Path(args.dist_dir)
    zips = sorted(dist.glob("*.zip"))
    if len(zips) < args.min_zips:
        print(f"[E2E][FAIL] Expected at least {args.min_zips} ZIP(s) in {dist}, found {len(zips)}")
        return 2

    print(f"[E2E][OK] Found {len(zips)} ZIP(s) in {dist}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
