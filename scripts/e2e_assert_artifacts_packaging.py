#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.e2e_assert_chaining import assert_artifacts_packaging


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Validate per-deliverable artifacts packaging (contract-only files + manifest)."
    )
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--since", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    ap.add_argument("--dist-dir", default="dist_artifacts")
    args = ap.parse_args()

    assert_artifacts_packaging(
        billing_state_dir=Path(args.billing_state_dir),
        since=args.since,
        tenant_id=args.tenant_id,
        work_order_id=args.work_order_id,
        dist_dir=Path(args.dist_dir),
    )

    print("[E2E][OK] Artifacts packaging validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
