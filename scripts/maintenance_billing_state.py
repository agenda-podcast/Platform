#!/usr/bin/env python3
"""Maintenance helper: ensure all repo tenants exist in tenants_credits.csv.

This script is intentionally *standalone* (no internal package imports) so it can be
executed in GitHub Actions with:
    python scripts/maintenance_billing_state.py --tenants-credits-csv <path>

Invariant:
  For every canonical tenant folder tenants/NNNNNNNNNN, there must be a row in
  tenants_credits.csv (even if credits_available=0).
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Dict, List

TENANT_ID_RE = re.compile(r"^\d{10}$")
TENANT_ANY_DIGITS_RE = re.compile(r"^\d+$")

HEADER = ["tenant_id", "credits_available", "updated_at", "status"]


def utcnow_iso() -> str:
    # RFC3339-ish; stable and readable for logs and diffs.
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def list_repo_tenants(tenants_dir: Path) -> List[str]:
    if not tenants_dir.exists():
        return []
    tids = []
    for p in tenants_dir.iterdir():
        if p.is_dir() and TENANT_ID_RE.match(p.name):
            tids.append(p.name)
    return sorted(set(tids))


def read_rows(path: Path) -> Dict[str, Dict[str, str]]:
    # NOTE: Excel/CSV editors may coerce 0000000001 -> 1. We canonicalize to
    # 10 digits and deterministically merge duplicates.
    rows: Dict[str, Dict[str, str]] = {}
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        # tolerate empty or missing header; will be rewritten below
        for r in reader:
            raw = (r.get("tenant_id") or "").strip()
            if not raw:
                continue

            tid = raw
            if TENANT_ANY_DIGITS_RE.match(raw):
                # Canonicalize to 10 digits (1 -> 0000000001)
                n = int(raw)
                if 0 < n < 10_000_000_000:
                    tid = f"{n:010d}"

            row = {k: (v or "").strip() for k, v in r.items()}
            row["tenant_id"] = tid

            # Deterministic merge if duplicates appear.
            if tid not in rows:
                rows[tid] = row
                continue

            prev = rows[tid]
            # Prefer ACTIVE status
            def _rank_status(x: str) -> int:
                return 1 if (x or "").strip().upper() == "ACTIVE" else 0

            def _credits(x: str) -> int:
                try:
                    return int((x or "0").strip() or 0)
                except Exception:
                    return 0

            cand_score = (
                _rank_status(row.get("status", "")),
                _credits(row.get("credits_available", "0")),
                (row.get("updated_at") or ""),
            )
            prev_score = (
                _rank_status(prev.get("status", "")),
                _credits(prev.get("credits_available", "0")),
                (prev.get("updated_at") or ""),
            )
            if cand_score >= prev_score:
                rows[tid] = row
    return rows


def write_rows(path: Path, rows: Dict[str, Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        for tid in sorted(rows.keys()):
            r = rows[tid]
            w.writerow(
                {
                    "tenant_id": tid,
                    "credits_available": str(r.get("credits_available") or "0"),
                    "updated_at": r.get("updated_at") or utcnow_iso(),
                    "status": r.get("status") or "ACTIVE",
                }
            )


def ensure_all_tenants(path: Path, tenants_dir: Path) -> int:
    repo_tenants = list_repo_tenants(tenants_dir)
    existing = read_rows(path)

    changed = 0
    now = utcnow_iso()
    for tid in repo_tenants:
        if tid not in existing:
            existing[tid] = {
                "tenant_id": tid,
                "credits_available": "0",
                "updated_at": now,
                "status": "ACTIVE",
            }
            changed += 1

    # Rewrite file (also normalizes header/ordering).
    write_rows(path, existing)
    return changed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenants-credits-csv", required=True, help="Path to tenants_credits.csv to update in-place")
    ap.add_argument("--tenants-dir", default="tenants", help="Repo tenants directory (default: tenants/)")
    args = ap.parse_args()

    credits_csv = Path(args.tenants_credits_csv)
    tenants_dir = Path(args.tenants_dir)

    changed = ensure_all_tenants(credits_csv, tenants_dir)
    print(f"[MAINTENANCE] tenants_credits backfill: {changed} rows added; path={credits_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
