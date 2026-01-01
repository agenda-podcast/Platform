from __future__ import annotations

import argparse
import csv
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

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

DEFAULT_EFFECTIVE_FROM = "1970-01-01"
DEFAULT_ACTIVE = "true"
DEFAULT_NOTES = "Auto-added by Maintenance using billing defaults."


def _normalize_mid(mid: str) -> str:
    mid = (mid or "").strip()
    if DIGITS_RE.match(mid):
        return f"{int(mid):03d}"
    return mid


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


def _load_defaults(config_path: Path) -> Tuple[int, int]:
    # Defaults if config is missing or unreadable.
    run_default = 5
    save_default = 2

    if not config_path.exists():
        return run_default, save_default

    if yaml is None:
        raise RuntimeError("PyYAML is required to read billing_config.yaml. Add 'pyyaml' to requirements.txt")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    bd = data.get("billing_defaults") or {}
    try:
        run_default = int(bd.get("default_price_run_credits", run_default))
        save_default = int(bd.get("default_price_save_to_release_credits", save_default))
    except Exception:
        # If config values are malformed, keep safe defaults.
        pass

    return run_default, save_default


def backfill_module_prices(module_prices_csv: Path, modules_dir: Path, run_default: int, save_default: int) -> Dict[str, int]:
    # module_prices.csv must already exist in repo. If missing, create it with correct header.
    if not module_prices_csv.exists():
        _write_csv(module_prices_csv, EXPECTED_HEADER, [])

    header, rows = _read_csv(module_prices_csv)
    if header and header != EXPECTED_HEADER:
        raise RuntimeError(
            f"CSV header mismatch for {module_prices_csv}:\n"
            f"  expected: {EXPECTED_HEADER}\n"
            f"  got:      {header}"
        )

    today = date.today()
    module_ids = _collect_module_ids(modules_dir)

    # Normalize + dedupe (single-row model per module_id)
    by_mid: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = _normalize_mid(r.get("module_id", ""))
        if not mid:
            continue
        r["module_id"] = mid
        if mid not in by_mid:
            by_mid[mid] = r
        else:
            # prefer effective row if duplicates
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
                "price_run_credits": str(run_default),
                "price_save_to_release_credits": str(save_default),
                "effective_from": DEFAULT_EFFECTIVE_FROM,
                "effective_to": "",
                "active": DEFAULT_ACTIVE,
                "notes": DEFAULT_NOTES,
            })
            idx[mid] = rows[-1]
            added += 1
            continue

        # Ensure row is usable (active + effective) but DO NOT overwrite explicit tenant pricing.
        r = idx[mid]
        before = dict(r)

        # Normalize id
        r["module_id"] = _normalize_mid(r.get("module_id", ""))

        # Ensure fields exist
        if (r.get("price_run_credits") or "").strip() == "":
            r["price_run_credits"] = str(run_default)
        if (r.get("price_save_to_release_credits") or "").strip() == "":
            r["price_save_to_release_credits"] = str(save_default)

        if (r.get("active") or "").strip().lower() not in ("true", "1", "yes", "y"):
            r["active"] = "true"

        ef = _parse_date(r.get("effective_from") or "")
        if ef is None or ef > today:
            r["effective_from"] = DEFAULT_EFFECTIVE_FROM

        et = _parse_date(r.get("effective_to") or "")
        if et is not None and et < today:
            r["effective_to"] = ""

        if r != before:
            updated += 1

    rows.sort(key=lambda r: r.get("module_id", ""))
    _write_csv(module_prices_csv, EXPECTED_HEADER, rows)
    return {"modules_seen": len(module_ids), "rows_added": added, "rows_updated": updated}


def main() -> int:
    ap = argparse.ArgumentParser(description="Maintenance: backfill platform/billing/module_prices.csv with defaults for any missing modules.")
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--module-prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--billing-config-path", default="platform/billing/billing_config.yaml")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    module_prices = Path(args.module_prices_path)
    billing_cfg = Path(args.billing_config_path)

    run_default, save_default = _load_defaults(billing_cfg)

    res = backfill_module_prices(module_prices, modules_dir, run_default, save_default)
    print(f"[MAINT][OK] module_prices backfill: {res}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
