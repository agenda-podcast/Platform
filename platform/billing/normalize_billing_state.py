"""Normalize IDs inside a billing-state directory.

This script is intentionally standalone so it can be invoked in CI or manually:

  python -m platform.billing.normalize_billing_state --billing-state-dir .billing-state

What it does:
- Reads known billing CSVs if they exist.
- Canonicalizes fixed-width numeric ID columns (e.g., tenant_id, module_id, reason_code)
  so that Excel-stripped values ("1") become repo-contract values ("0000000001").
- Dedupes certain key tables deterministically.
- Writes the CSVs back.

Rationale:
CSV has no types; Excel/pandas often strip leading zeros. Canonicalizing on write prevents drift.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable

from platform.common.id_normalize import canonicalize_row_ids, dedupe_rows_by_normalized_id


# Configure which files/columns are IDs.
"""File -> list of id-like fields.

We only canonicalize fields that are defined in BILLING_CSV_ID_WIDTHS.
Other listed fields are whitespace-trimmed only (safe for alphanumeric IDs).
"""

BILLING_CSV_ID_FIELDS: dict[str, list[str]] = {
    "tenants_credits.csv": ["tenant_id"],
    "transactions.csv": ["transaction_id", "tenant_id", "work_order_id"],
    "transaction_items.csv": ["transaction_item_id", "transaction_id", "tenant_id", "work_order_id", "module_run_id", "reason_code"],
    "promotion_redemptions.csv": ["event_id", "tenant_id", "promo_id", "work_order_id"],
    "workorders_log.csv": ["work_order_id", "tenant_id"],
    "module_runs_log.csv": ["module_run_id", "work_order_id", "tenant_id", "module_id", "reason_code"],
    "payments.csv": ["payment_id", "tenant_id", "topup_method_id"],
    "topup_instructions.csv": ["topup_method_id"],
}

# Per-field fixed widths for digits-only IDs.
BILLING_CSV_ID_WIDTHS: dict[str, int] = {
    "tenant_id": 10,
    "module_id": 6,
    "reason_code": 12,
}

# Which tables require dedupe by ID after normalization.
DEDUPE_TABLES: dict[str, dict[str, Any]] = {
    "tenants_credits.csv": {"id_field": "tenant_id", "prefer": "latest", "timestamp_fields": ("updated_at", "modified_at", "created_at")},
    "topup_instructions.csv": {"id_field": "topup_method_id", "prefer": "last", "timestamp_fields": ()},
}


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = [dict(r) for r in reader]
    return headers, rows


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            # ensure all values are strings for csv
            out = {k: ("" if v is None else str(v)) for k, v in r.items()}
            writer.writerow(out)


def normalize_billing_state(billing_state_dir: str) -> list[str]:
    base = Path(billing_state_dir)
    if not base.exists() or not base.is_dir():
        raise SystemExit(f"billing-state-dir not found or not a directory: {billing_state_dir}")

    notes: list[str] = []

    for filename, id_fields in BILLING_CSV_ID_FIELDS.items():
        p = base / filename
        if not p.exists():
            continue

        headers, rows = _read_csv(p)

        # Canonicalize ID fields (fixed-width padding) for numeric ids;
        # trim whitespace for alphanumeric ids.
        normed = [canonicalize_row_ids(r, id_fields, widths=BILLING_CSV_ID_WIDTHS) for r in rows]

        # Deterministic dedupe if configured
        if filename in DEDUPE_TABLES:
            cfg = DEDUPE_TABLES[filename]
            res = dedupe_rows_by_normalized_id(
                normed,
                cfg["id_field"],
                prefer=cfg.get("prefer", "latest"),
                timestamp_fields=cfg.get("timestamp_fields", ()),
            )
            normed = res.rows
            if res.merged_count:
                notes.append(
                    f"{filename}: normalized + deduped by {cfg['id_field']} (duplicates merged={res.merged_count}, dropped={res.dropped_count})"
                )

        # Preserve header order; add any missing normalized id headers if needed
        # (Do not reorder unless necessary.)
        _write_csv(p, headers or (list(normed[0].keys()) if normed else headers), normed)
        notes.append(f"{filename}: canonicalized IDs for columns {id_fields}")

    return notes


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    args = ap.parse_args(list(argv) if argv is not None else None)

    notes = normalize_billing_state(args.billing_state_dir)
    for n in notes:
        print(n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
