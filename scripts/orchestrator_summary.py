\
from __future__ import annotations

import argparse
import json
from pathlib import Path
import csv
import sys
from typing import Dict, List, Optional


REQ = [
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


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def _tail(rows: List[Dict[str, str]], n: int) -> List[Dict[str, str]]:
    if n <= 0:
        return []
    return rows[-n:] if len(rows) > n else rows


def _print_table(title: str, rows: List[Dict[str, str]], cols: List[str], max_rows: int = 10) -> None:
    print("")
    print("=" * 80)
    print(title)
    print("=" * 80)
    if not rows:
        print("(no rows)")
        return
    rows = _tail(rows, max_rows)
    # Compute widths
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        print(" | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", default=".billing-state")
    ap.add_argument("--runtime-dir", default="runtime")
    ap.add_argument("--tenants-dir", default="tenants")
    args = ap.parse_args()

    bdir = Path(args.billing_state_dir)
    rdir = Path(args.runtime_dir)
    tdir = Path(args.tenants_dir)

    print("")
    print("ORCHESTRATOR SUMMARY")
    print(f"billing_state_dir: {bdir.resolve()}")
    print(f"runtime_dir:       {rdir.resolve()}")
    print(f"tenants_dir:       {tdir.resolve()}")

    # Check required billing-state files exist
    missing = [f for f in REQ if not (bdir / f).exists()]
    if missing:
        print("")
        print(f"WARNING: billing-state missing files after run: {missing}")
        print("Directory listing:")
        for p in sorted(bdir.glob("*")):
            print(f" - {p.name}")
        # Do not hard-fail here; orchestrator already succeeded. Keep summary informative.
    # Discover workorders
    workorders = sorted(tdir.glob("*/workorders/*.yml"))
    print("")
    print(f"Discovered workorders: {len(workorders)}")
    for p in workorders[:20]:
        print(f" - {p}")

    # Show billing logs tail
    wol = _read_csv(bdir / "workorders_log.csv")
    mrl = _read_csv(bdir / "module_runs_log.csv")
    _print_table("Workorders Log (tail)", wol, cols=[k for k in (wol[0].keys() if wol else ["work_order_id","tenant_id","status","created_at","started_at","ended_at","note"])], max_rows=10)
    _print_table("Module Runs Log (tail)", mrl, cols=[k for k in (mrl[0].keys() if mrl else ["module_run_id","tenant_id","work_order_id","module_id","status","created_at","started_at","ended_at","reason_code","output_ref"])], max_rows=15)

    # Show runtime tree (top-level)
    print("")
    print("Runtime directory snapshot:")
    if rdir.exists():
        for p in sorted(rdir.glob("*")):
            if p.is_dir():
                print(f" - {p.name}/")
            else:
                print(f" - {p.name} ({p.stat().st_size} bytes)")
    else:
        print(" (runtime dir not found)")

    # Show tenant outputs snapshot
    print("")
    print("Tenant outputs snapshot:")
    if tdir.exists():
        for tenant in sorted([p for p in tdir.iterdir() if p.is_dir()])[:50]:
            outdir = tenant / "outputs"
            if outdir.exists():
                items = list(outdir.rglob("*"))
                print(f" - {tenant.name}: outputs present ({len(items)} items)")
            else:
                print(f" - {tenant.name}: no outputs/ directory")
    else:
        print(" (tenants dir not found)")
    print("")
    return 0


if __name__ == "__main__":
    sys.exit(main())
