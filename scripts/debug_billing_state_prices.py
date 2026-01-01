from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

PRICES_HEADER = ["module_id","price_run_credits","price_save_to_release_credits","effective_from","effective_to","active","notes"]

def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", default=".billing-state-ci")
    ap.add_argument("--module-id", default="001")
    args = ap.parse_args()

    prices = Path(args.billing_state_dir) / "module_prices.csv"
    if not prices.exists():
        print(f"[DEBUG] Missing: {prices}")
        return 2

    header, rows = read_csv(prices)
    print(f"[DEBUG] module_prices.csv header: {header}")
    if header != PRICES_HEADER:
        print(f"[DEBUG] header mismatch vs expected: {PRICES_HEADER}")
        return 2

    for r in rows:
        if (r.get("module_id") or "") == args.module_id:
            print(f"[DEBUG] Found row for {args.module_id}: {r}")
            return 0

    print(f"[DEBUG] No row for module {args.module_id} in {prices}")
    return 2

if __name__ == "__main__":
    raise SystemExit(main())
