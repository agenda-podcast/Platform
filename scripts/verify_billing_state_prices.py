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

def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])

def _parse_date(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        y,m,d = s.split("-")
        return date(int(y),int(m),int(d))
    except Exception:
        return None

def _is_effective_now(r: Dict[str, str], today: date) -> bool:
    active = (r.get("active") or "").strip().lower() in ("true","1","yes","y")
    if not active:
        return False
    ef = _parse_date(r.get("effective_from") or "")
    et = _parse_date(r.get("effective_to") or "")
    if ef and ef > today:
        return False
    if et and et < today:
        return False
    return True

def _collect_module_ids(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    for p in modules_dir.iterdir():
        if p.is_dir():
            m = MODULE_ID_RE.match(p.name)
            if m:
                ids.append(m.group(1))
    return sorted(set(ids))

def main() -> int:
    ap = argparse.ArgumentParser(description="Verify billing-state module_prices.csv includes effective+active prices for all modules.")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--billing-state-dir", default=".billing-state-ci")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    bs = Path(args.billing_state_dir)

    if not modules_dir.exists():
        print("[VERIFY_PRICES][FAIL] modules dir missing")
        return 2
    if not bs.exists():
        print("[VERIFY_PRICES][FAIL] billing-state dir missing")
        return 2

    module_ids = _collect_module_ids(modules_dir)
    if not module_ids:
        print("[VERIFY_PRICES][OK] no modules to verify")
        return 0

    # Find all module_prices.csv under billing-state and pick the first with correct header
    candidates = sorted(bs.rglob("module_prices.csv"))
    if not candidates:
        print("[VERIFY_PRICES][FAIL] no module_prices.csv found under billing-state")
        return 2

    today = date.today()
    usable = None
    usable_rows: List[Dict[str, str]] = []
    for p in candidates:
        try:
            header, rows = _read_csv(p)
        except Exception:
            continue
        if header == EXPECTED_HEADER:
            usable = p
            usable_rows = rows
            break

    if usable is None:
        print("[VERIFY_PRICES][FAIL] no module_prices.csv with expected header found under billing-state")
        return 2

    rows_by_mid = {}
    for r in usable_rows:
        mid = (r.get("module_id") or "").strip()
        if mid:
            rows_by_mid[mid] = r

    missing = []
    not_effective = []
    for mid in module_ids:
        r = rows_by_mid.get(mid)
        if not r:
            missing.append(mid)
            continue
        if not _is_effective_now(r, today):
            not_effective.append(mid)

    if missing or not_effective:
        if missing:
            print(f"[VERIFY_PRICES][FAIL] missing module prices for: {missing}")
        if not_effective:
            print(f"[VERIFY_PRICES][FAIL] prices not effective/active for: {not_effective}")
        print(f"[VERIFY_PRICES] usable file: {usable}")
        return 2

    print(f"[VERIFY_PRICES][OK] billing-state prices present and effective for all modules. file={usable}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
