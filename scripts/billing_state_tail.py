#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def tail_rows(path: Path, n: int) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    return rows[-n:]


def print_table(path: Path, n: int) -> None:
    print("\n" + "=" * 100)
    print(f"{path.name}  (exists={path.exists()})")
    if not path.exists():
        print("<missing>")
        return
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
    print(f"header: {header}")
    rows = tail_rows(path, n)
    print(f"rows_total_tail={len(rows)}")
    for r in rows:
        # print a compact view
        keys = list(r.keys())
        compact = ", ".join([f"{k}={r.get(k,'')}" for k in keys[: min(6, len(keys))]])
        print(f"- {compact}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--n", type=int, default=20)
    args = ap.parse_args()

    bdir = Path(args.billing_state_dir).resolve()
    print("\n[BILLING_STATE_TAIL] billing_state_dir=", bdir)

    for fname in [
        "tenants_credits.csv",
        "transactions.csv",
        "transaction_items.csv",
        "workorders_log.csv",
        "module_runs_log.csv",
        "cache_index.csv",
        "github_releases_map.csv",
        "github_assets_map.csv",
    ]:
        print_table(bdir / fname, args.n)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
