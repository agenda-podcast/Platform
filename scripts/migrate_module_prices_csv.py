# Patch: safer migration (effective_from set to 1970-01-01 to avoid "future price" failures)
from __future__ import annotations

import argparse
import csv
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

LEGACY_HEADER = [
    "module_id",
    "price_unit",
    "price_credits",
    "price_scope",
    "note",
]

DEFAULT_EFFECTIVE_FROM = "1970-01-01"


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])


def write_csv(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def migrate_legacy_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in rows:
        module_id = (r.get("module_id") or "").strip()
        if not module_id:
            continue

        price_credits = (r.get("price_credits") or "").strip()
        unit = (r.get("price_unit") or "").strip()
        scope = (r.get("price_scope") or "").strip()
        note = (r.get("note") or "").strip()

        notes = "; ".join([x for x in [
            note,
            f"legacy_unit={unit}" if unit else "",
            f"legacy_scope={scope}" if scope else "",
            "migrated_from_legacy_header"
        ] if x])

        out.append({
            "module_id": module_id,
            "price_run_credits": price_credits if price_credits else "0",
            "price_save_to_release_credits": "0",
            "effective_from": DEFAULT_EFFECTIVE_FROM,
            "effective_to": "",
            "active": "true",
            "notes": notes,
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Migrate module_prices.csv legacy schema -> platform schema.")
    ap.add_argument("--path", default="platform/billing/module_prices.csv")
    ap.add_argument("--out", default="", help="Optional output path (if set, does not overwrite input).")
    args = ap.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    header, rows = read_csv(path)
    if header == EXPECTED_HEADER:
        print("No migration needed: header already matches expected schema.")
        return 0
    if header != LEGACY_HEADER:
        raise SystemExit(f"Cannot migrate: unexpected header. expected legacy={LEGACY_HEADER} got={header}")

    new_rows = migrate_legacy_rows(rows)
    out_path = Path(args.out) if args.out else path
    write_csv(out_path, EXPECTED_HEADER, new_rows)
    print(f"Migrated {len(new_rows)} row(s). Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
