from __future__ import annotations

import argparse
import csv
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

EXPECTED_HEADER = [
  "module_id",
  "price_run_credits",
  "price_save_to_release_credits",
  "effective_from",
  "effective_to",
  "active",
  "notes",
]

MODULE_ID_RE = re.compile(r"^(\d{3})_")

def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])

def parse_date(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        y,m,d = s.split("-")
        return date(int(y),int(m),int(d))
    except Exception:
        return None

def is_effective_now(r: Dict[str, str], today: date) -> bool:
    active = (r.get("active") or "").strip().lower() in ("true","1","yes","y")
    if not active:
        return False
    ef = parse_date(r.get("effective_from") or "")
    et = parse_date(r.get("effective_to") or "")
    if ef and ef > today:
        return False
    if et and et < today:
        return False
    return True

def collect_module_ids(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    for p in modules_dir.iterdir():
        if p.is_dir():
            m = MODULE_ID_RE.match(p.name)
            if m:
                ids.append(m.group(1))
    return sorted(set(ids))

def main() -> int:
    ap = argparse.ArgumentParser(description="Verify Maintenance produced module_prices seed covering all modules.")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--seed-path", default="platform/billing_state_seed/module_prices.csv")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    seed = Path(args.seed_path)

    if not modules_dir.exists():
        print("[VERIFY][FAIL] modules dir missing")
        return 2
    if not seed.exists():
        print(f"[VERIFY][FAIL] seed module_prices.csv missing: {seed}")
        return 2

    header, rows = read_csv(seed)
    if header != EXPECTED_HEADER:
        print(f"[VERIFY][FAIL] seed header mismatch. expected={EXPECTED_HEADER} got={header}")
        return 2

    today = date.today()
    module_ids = collect_module_ids(modules_dir)
    by_mid = { (r.get("module_id") or "").strip(): r for r in rows if (r.get("module_id") or "").strip() }

    missing = [mid for mid in module_ids if mid not in by_mid]
    not_effective = [mid for mid in module_ids if mid in by_mid and not is_effective_now(by_mid[mid], today)]

    if missing or not_effective:
        if missing:
            print(f"[VERIFY][FAIL] missing module ids in seed: {missing}")
        if not_effective:
            print(f"[VERIFY][FAIL] non-effective/inactive module ids in seed: {not_effective}")
        return 2

    print("[VERIFY][OK] maintenance seed prices cover all modules and are effective+active.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
