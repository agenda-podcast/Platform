"""Canonicalize IDs inside a billing-state directory.

Usage:
  python -m platform.billing.normalize_billing_state --billing-state-dir .billing-state

Why:
- CSV has no types; Excel/pandas often strip leading zeros from digit-only identifiers.
- This platform has a *repo contract* (validated by scripts/ci_verify.py):
    tenant_id = 10 digits (zero padded)
    module_id  = 6 digits (zero padded)
- This tool repairs billing-state files in-place by converting IDs back to canonical forms and
  deterministically merging duplicate key rows that arise after normalization.

Deterministic merge policy:
- For key tables where the ID is effectively a primary key (e.g., tenants_credits.csv),
  if multiple rows collapse to the same canonical ID, keep the "best" row:
    1) Prefer the most recent updated_at when present
    2) If tie/missing, prefer higher credits_available
    3) Final tie-break: later row in file
- We do NOT sum balances across duplicates (avoids accidental double counting).

Files handled:
- tenants_credits.csv
- transactions.csv
- payments.csv
- topup_instructions.csv
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any, Iterable

from platform.common.id_canonical import normalize_tenant_id


BILLING_CSV_ID_FIELDS: dict[str, list[str]] = {
    "tenants_credits.csv": ["tenant_id"],
    "transactions.csv": ["transaction_id", "tenant_id", "work_order_id"],
    "payments.csv": ["payment_id", "tenant_id", "topup_method_id"],
    "topup_instructions.csv": ["topup_method_id"],
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
            out = {k: ("" if v is None else str(v)) for k, v in r.items()}
            writer.writerow(out)


def _canonicalize_row_ids(row: dict[str, str], id_fields: list[str]) -> dict[str, str]:
    # For now, only tenant_id has a fixed-width canonical form.
    # Other IDs (transaction_id, payment_id, work_order_id, topup_method_id) may be opaque strings.
    out = dict(row)
    for f in id_fields:
        if f not in out:
            continue
        if f in ("tenant_id", "source_tenant_id", "target_tenant_id"):
            out[f] = normalize_tenant_id(out.get(f, ""))
        else:
            out[f] = "" if out.get(f) is None else str(out.get(f)).strip()
    return out


def _canonicalize_tenants_credits(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    best: dict[str, dict[str, str]] = {}
    best_meta: dict[str, tuple[str, int, int]] = {}  # (ts, credits, idx)

    for idx, r in enumerate(rows):
        tid = normalize_tenant_id(r.get("tenant_id", ""))
        if not tid:
            continue
        r["tenant_id"] = tid
        ts = str(r.get("updated_at", "")).strip()
        try:
            credits = int(str(r.get("credits_available", "0")).strip() or 0)
        except Exception:
            credits = 0

        if tid not in best:
            best[tid] = r
            best_meta[tid] = (ts, credits, idx)
            continue

        prev_ts, prev_credits, prev_idx = best_meta[tid]
        take = False
        if ts and prev_ts:
            if ts > prev_ts:
                take = True
            elif ts < prev_ts:
                take = False
            else:
                if credits > prev_credits:
                    take = True
                elif credits < prev_credits:
                    take = False
                else:
                    take = idx > prev_idx
        elif ts and not prev_ts:
            take = True
        elif not ts and prev_ts:
            take = False
        else:
            if credits > prev_credits:
                take = True
            elif credits < prev_credits:
                take = False
            else:
                take = idx > prev_idx

        if take:
            best[tid] = r
            best_meta[tid] = (ts, credits, idx)

    return [best[k] for k in sorted(best.keys())]


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
        normed = [_canonicalize_row_ids(r, id_fields) for r in rows]

        if filename == "tenants_credits.csv":
            normed = _canonicalize_tenants_credits(normed)
            notes.append(f"{filename}: canonicalized tenant_id and merged duplicates (deterministic)")
        else:
            notes.append(f"{filename}: canonicalized ID columns {id_fields}")

        # Preserve existing header order; if header missing (unlikely), infer from first row.
        out_headers = headers or (list(normed[0].keys()) if normed else headers)
        _write_csv(p, out_headers, normed)

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
