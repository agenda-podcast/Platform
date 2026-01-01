from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import yaml  # PyYAML
except Exception:
    yaml = None

MODULE_ID_RE = re.compile(r"^(\d{3})_")
DIGITS_RE = re.compile(r"^\d+$")

# Canonical header expected by scripts/ci_verify.py
PRICES_HEADER = ["module_id","price_run_credits","price_save_to_release_credits","effective_from","effective_to","active","notes"]

DEFAULT_EFFECTIVE_FROM = "1970-01-01"


def _normalize_mid(mid: str) -> str:
    mid = (mid or "").strip()
    if DIGITS_RE.match(mid):
        # force 3-digit ID for consistency with module folders (001, 002, ...)
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


def _ensure_csv(path: Path, header: List[str]) -> None:
    if not path.exists():
        _write_csv(path, header, [])
        return
    got, _ = _read_csv(path)
    if got and got != header:
        raise RuntimeError(f"CSV header mismatch for {path}: expected {header} got {got}")


def list_module_dirs(modules_dir: Path) -> List[Path]:
    return [p for p in modules_dir.iterdir() if p.is_dir()]


def parse_module_ids_from_folders(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if m:
            ids.append(m.group(1))
    return sorted(set(ids))


def _is_effective_now(row: Dict[str, str], today: date) -> bool:
    active = (row.get("active") or "").strip().lower() in ("true","1","yes","y")
    if not active:
        return False

    ef = (row.get("effective_from") or "").strip()
    et = (row.get("effective_to") or "").strip()

    def _parse(d: str) -> date:
        y, m, dd = d.split("-")
        return date(int(y), int(m), int(dd))

    if ef:
        try:
            if _parse(ef) > today:
                return False
        except Exception:
            return False

    if et:
        try:
            if _parse(et) < today:
                return False
        except Exception:
            return False

    return True


def ensure_prices_for_all_modules(prices_csv: Path, module_ids: List[str], defaults_by_id: Dict[str, Dict[str, str]] | None = None) -> Dict[str, Any]:
    defaults_by_id = defaults_by_id or {}
    _ensure_csv(prices_csv, PRICES_HEADER)
    _, rows = _read_csv(prices_csv)

    today = date.today()

    # Normalize existing module_id values (pad to 3 digits) and de-duplicate
    normalized: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = _normalize_mid(r.get("module_id",""))
        if not mid:
            continue
        r["module_id"] = mid
        # Keep the first effective row if multiple; we will overwrite below as needed.
        if mid not in normalized:
            normalized[mid] = r
        else:
            # Prefer an effective row if possible.
            if _is_effective_now(r, today) and not _is_effective_now(normalized[mid], today):
                normalized[mid] = r

    rows = list(normalized.values())

    idx = {r["module_id"]: r for r in rows}
    added = 0
    updated = 0

    for mid in module_ids:
        desired = defaults_by_id.get(mid, {})
        desired_row = {
            "module_id": mid,
            "price_run_credits": str(desired.get("price_run_credits", "1")),
            "price_save_to_release_credits": str(desired.get("price_save_to_release_credits", "0")),
            "effective_from": str(desired.get("effective_from", DEFAULT_EFFECTIVE_FROM)),
            "effective_to": str(desired.get("effective_to", "")),
            "active": str(desired.get("active", "true")).lower() if str(desired.get("active","true")).lower() in ("true","false") else "true",
            "notes": str(desired.get("notes", "Backfilled by maintenance helper.")),
        }

        row = idx.get(mid)
        if row is None:
            rows.append(desired_row)
            idx[mid] = desired_row
            added += 1
            continue

        # If current row is not effective now, overwrite to make it effective.
        if not _is_effective_now(row, today):
            for k, v in desired_row.items():
                row[k] = v
            updated += 1
        else:
            # Fill missing fields
            changed = False
            for k, v in desired_row.items():
                if (row.get(k) or "") == "" and v != "":
                    row[k] = v
                    changed = True
            if changed:
                updated += 1

    _write_csv(prices_csv, PRICES_HEADER, sorted(rows, key=lambda r: r.get("module_id","")))
    return {"path": str(prices_csv), "rows_added": added, "rows_updated": updated, "modules_count": len(module_ids)}


def resolve_billing_state_price_paths(billing_state_dir: Path) -> List[Path]:
    # Write to multiple plausible locations because different codepaths may load different roots.
    return [
        billing_state_dir / "module_prices.csv",
        billing_state_dir / "billing" / "module_prices.csv",
        billing_state_dir / "platform" / "billing" / "module_prices.csv",
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--repo-prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--billing-state-dir", default="", help="If set, also writes module_prices.csv into billing-state (for orchestrate runtime).")
    ap.add_argument("--report-path", default="runtime/maintenance_prices_report.json")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    if not modules_dir.exists():
        raise RuntimeError(f"Modules dir not found: {modules_dir}")

    module_ids = parse_module_ids_from_folders(modules_dir)

    # We can optionally load per-module defaults from module.yaml pricing if you want,
    # but the critical fix is presence + effective window. Keep defaults simple here.
    defaults_by_id: Dict[str, Dict[str, str]] = {}

    report: Dict[str, Any] = {"module_ids": module_ids, "repo_prices": {}, "billing_state_prices": []}

    # 1) Ensure repo prices (for CI schema checks)
    report["repo_prices"] = ensure_prices_for_all_modules(Path(args.repo_prices_path), module_ids, defaults_by_id)

    # 2) Ensure billing-state prices (for orchestrate)
    if args.billing_state_dir:
        bs = Path(args.billing_state_dir)
        bs.mkdir(parents=True, exist_ok=True)
        for p in resolve_billing_state_price_paths(bs):
            p.parent.mkdir(parents=True, exist_ok=True)
            report["billing_state_prices"].append(ensure_prices_for_all_modules(p, module_ids, defaults_by_id))

    out = Path(args.report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
