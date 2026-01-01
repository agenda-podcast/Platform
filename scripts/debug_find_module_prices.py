from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

EXPECTED_HEADER = ["module_id","price_run_credits","price_save_to_release_credits","effective_from","effective_to","active","notes"]

def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])

def is_effective_now(r: Dict[str, str]) -> bool:
    today = date.today()
    active = (r.get("active") or "").strip().lower() in ("true","1","yes","y")
    if not active:
        return False
    ef = (r.get("effective_from") or "").strip()
    et = (r.get("effective_to") or "").strip()
    def parse(d: str):
        y,m,dd = d.split("-")
        return date(int(y),int(m),int(dd))
    if ef:
        try:
            if parse(ef) > today:
                return False
        except Exception:
            return False
    if et:
        try:
            if parse(et) < today:
                return False
        except Exception:
            return False
    return True

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", default=".billing-state-ci")
    ap.add_argument("--module-id", default="001")
    args = ap.parse_args()

    bs = Path(args.billing_state_dir)
    if not bs.exists():
        print(f"[DEBUG] billing-state-dir does not exist: {bs}")
        return 2

    hits = sorted(bs.rglob("module_prices.csv"))
    if not hits:
        print(f"[DEBUG] No module_prices.csv found under {bs}")
        return 2

    for p in hits:
        try:
            header, rows = read_csv(p)
        except Exception as e:
            print(f"[DEBUG] {p}: failed to read: {e}")
            continue

        print(f"[DEBUG] FOUND: {p}")
        print(f"  header: {header}")
        if header != EXPECTED_HEADER:
            print(f"  status: HEADER_MISMATCH (expected {EXPECTED_HEADER})")
            continue

        # Locate module row
        row = None
        for r in rows:
            if (r.get("module_id") or "").strip() == args.module_id:
                row = r
                break

        if not row:
            # try numeric normalization symptom (e.g., '1' instead of '001')
            for r in rows:
                if (r.get("module_id") or "").strip().lstrip("0") == args.module_id.lstrip("0"):
                    print(f"  WARNING: module id present but not zero-padded: {r.get('module_id')} (expected {args.module_id})")
                    row = r
                    break

        if not row:
            print(f"  status: MISSING_ROW for module {args.module_id}")
            continue

        eff = is_effective_now(row)
        print(f"  row: {row}")
        print(f"  status: {'EFFECTIVE_ACTIVE' if eff else 'NOT_EFFECTIVE_OR_INACTIVE'}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
