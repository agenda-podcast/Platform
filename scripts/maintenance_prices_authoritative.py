from __future__ import annotations

import argparse
import csv
import re
import json
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
DIGITS_RE = re.compile(r"^\d+$")

DEFAULT_RUN = 5
DEFAULT_SAVE = 2
DEFAULT_EFFECTIVE_FROM = "1970-01-01"
DEFAULT_ACTIVE = "true"
DEFAULT_NOTES = "Auto-added by Maintenance: default pricing for module."


def _normalize_mid(mid: str) -> str:
    mid = (mid or "").strip()
    if DIGITS_RE.match(mid):
        return f"{int(mid):03d}"
    return mid


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])


def _write_csv(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def _parse_date(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def _is_effective_now(row: Dict[str, str], today: date) -> bool:
    active = (row.get("active") or "").strip().lower() in ("true", "1", "yes", "y")
    if not active:
        return False
    ef = _parse_date(row.get("effective_from") or "")
    et = _parse_date(row.get("effective_to") or "")
    if ef and ef > today:
        return False
    if et and et < today:
        return False
    return True


def _collect_module_ids(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    if not modules_dir.exists():
        return ids
    for p in modules_dir.iterdir():
        if not p.is_dir():
            continue
        m = MODULE_ID_RE.match(p.name)
        if m:
            ids.append(m.group(1))
    return sorted(set(ids))


def _ensure_prices_table(path: Path, module_ids: List[str], *, default_run: int, default_save: int) -> Dict[str, int]:
    # Ensure file exists with correct header
    if not path.exists():
        _write_csv(path, EXPECTED_HEADER, [])

    header, rows = _read_csv(path)
    if header and header != EXPECTED_HEADER:
        raise RuntimeError(f"Header mismatch for {path}: expected={EXPECTED_HEADER} got={header}")

    today = date.today()

    # normalize + dedupe (single-row model by module_id)
    by_mid: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = _normalize_mid(r.get("module_id", ""))
        if not mid:
            continue
        r["module_id"] = mid
        if mid not in by_mid:
            by_mid[mid] = r
        else:
            if _is_effective_now(r, today) and not _is_effective_now(by_mid[mid], today):
                by_mid[mid] = r

    rows = list(by_mid.values())
    idx = {r["module_id"]: r for r in rows}

    added = 0
    updated = 0

    for mid in module_ids:
        if mid not in idx:
            rows.append({
                "module_id": mid,
                "price_run_credits": str(default_run),
                "price_save_to_release_credits": str(default_save),
                "effective_from": DEFAULT_EFFECTIVE_FROM,
                "effective_to": "",
                "active": DEFAULT_ACTIVE,
                "notes": DEFAULT_NOTES,
            })
            idx[mid] = rows[-1]
            added += 1
            continue

        # Make effective+active now (authoritative maintenance guarantee)
        r = idx[mid]
        before = dict(r)

        r["module_id"] = _normalize_mid(r.get("module_id", ""))

        # fill missing
        if (r.get("price_run_credits") or "").strip() == "":
            r["price_run_credits"] = str(default_run)
        if (r.get("price_save_to_release_credits") or "").strip() == "":
            r["price_save_to_release_credits"] = str(default_save)

        # active
        if (r.get("active") or "").strip().lower() not in ("true", "1", "yes", "y"):
            r["active"] = "true"

        # effective window
        ef = _parse_date(r.get("effective_from") or "")
        if ef is None or ef > today:
            r["effective_from"] = DEFAULT_EFFECTIVE_FROM
        et = _parse_date(r.get("effective_to") or "")
        if et is not None and et < today:
            r["effective_to"] = ""

        if r != before:
            updated += 1

    rows.sort(key=lambda r: r.get("module_id", ""))
    _write_csv(path, EXPECTED_HEADER, rows)
    return {"added": added, "updated": updated}


def main() -> int:
    ap = argparse.ArgumentParser(description="Maintenance: ensure all modules exist in price table and generate billing-state seed.")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--repo-prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--seed-dir", default="platform/billing_state_seed")
    ap.add_argument("--default-run-credits", type=int, default=DEFAULT_RUN)
    ap.add_argument("--default-save-to-release-credits", type=int, default=DEFAULT_SAVE)
    ap.add_argument("--report-path", default="runtime/maintenance_prices_report.json")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    module_ids = _collect_module_ids(modules_dir)
    if not module_ids:
        print("[MAINT_PRICES][OK] No numeric module folders found; nothing to do.")
        return 0

    repo_path = Path(args.repo_prices_path)
    seed_dir = Path(args.seed_dir)
    seed_prices = seed_dir / "module_prices.csv"

    repo_res = _ensure_prices_table(repo_path, module_ids, default_run=args.default_run_credits, default_save=args.default_save_to_release_credits)
    seed_res = _ensure_prices_table(seed_prices, module_ids, default_run=args.default_run_credits, default_save=args.default_save_to_release_credits)

    report = {
        "module_ids": module_ids,
        "repo_prices": {"path": str(repo_path), **repo_res},
        "seed_prices": {"path": str(seed_prices), **seed_res},
    }

    out = Path(args.report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[MAINT_PRICES][OK] " + json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
