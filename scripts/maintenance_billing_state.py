#!/usr/bin/env python3
"""Maintenance: billing-state servicing helpers.

This script is intentionally small and single-purpose.

Current responsibilities
------------------------
1) Ensure *every* canonical tenant in repo has a row in tenants_credits.csv
   (credits_available may be 0).

Rationale
---------
The orchestrator loads billing-state from GitHub Release assets (or local seed).
Orchestration correctly hard-fails when a tenant is missing from tenants_credits.csv
because the billing ledger must be complete.

Maintaining this invariant belongs to the platform servicing layer.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List

from platform.utils.time import utcnow_iso
TENANT_ID_RE = re.compile(r"^[0-9]{10}$")


TENANTS_CREDITS_HEADER = ["tenant_id", "credits_available", "updated_at", "status"]


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        rows: List[Dict[str, str]] = []
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
        return rows


def _write_csv_rows(path: Path, rows: List[Dict[str, str]], header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})


def discover_repo_tenants(repo_root: Path) -> List[str]:
    """Canonical tenants: folders under tenants/ that match 10-digit id and contain tenant.yml."""
    tdir = repo_root / "tenants"
    out: List[str] = []
    if not tdir.exists():
        return out
    for p in sorted(tdir.iterdir()):
        if not p.is_dir():
            continue
        tid = p.name.strip()
        if not TENANT_ID_RE.match(tid):
            continue
        if not (p / "tenant.yml").exists():
            continue
        out.append(tid)
    return out


def sync_tenants_credits(tenants_credits_csv: Path, tenant_ids: List[str]) -> bool:
    """Ensure tenant_ids exist in tenants_credits_csv. Returns True if changed."""
    now = utcnow_iso()
    existing = _read_csv_rows(tenants_credits_csv)

    by_id: Dict[str, Dict[str, str]] = {}
    for r in existing:
        tid = (r.get("tenant_id") or "").strip()
        if not tid:
            continue
        by_id[tid] = r

    changed = False
    for tid in tenant_ids:
        if tid not in by_id:
            by_id[tid] = {
                "tenant_id": tid,
                "credits_available": "0",
                "updated_at": now,
                "status": "ACTIVE",
            }
            changed = True
        else:
            # Normalize minimal fields without changing balances.
            r = by_id[tid]
            if (r.get("status") or "").strip() == "":
                r["status"] = "ACTIVE"
                changed = True
            if (r.get("updated_at") or "").strip() == "":
                r["updated_at"] = now
                changed = True
            if (r.get("credits_available") or "").strip() == "":
                r["credits_available"] = "0"
                changed = True

    # Preserve extra rows (tenants not currently in repo) for safety.
    rows_out = [by_id[k] for k in sorted(by_id.keys())]

    # If file did not exist, this is a change.
    if not tenants_credits_csv.exists():
        changed = True

    if changed:
        _write_csv_rows(tenants_credits_csv, rows_out, TENANTS_CREDITS_HEADER)
    return changed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Repository root (default: project root)",
    )
    ap.add_argument(
        "--tenants-credits-csv",
        required=True,
        help="Path to tenants_credits.csv to sync",
    )
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    tenants_credits_csv = Path(args.tenants_credits_csv).resolve()

    tenant_ids = discover_repo_tenants(repo_root)
    if not tenant_ids:
        print("[MAINTENANCE][WARN] No tenants discovered; nothing to sync")
        return 0

    changed = sync_tenants_credits(tenants_credits_csv, tenant_ids)
    if changed:
        print(f"[MAINTENANCE][OK] Synced tenants_credits.csv: {tenants_credits_csv}")
    else:
        print(f"[MAINTENANCE][OK] tenants_credits.csv already up-to-date: {tenants_credits_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
