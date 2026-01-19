from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

from ..utils.time import utcnow_iso


TENANTS_CREDITS_HEADERS = ["tenant_id", "credits_available", "updated_at", "status"]
TRANSACTIONS_HEADERS = [
    "transaction_id",
    "tenant_id",
    "work_order_id",
    "type",
    "amount_credits",
    "created_at",
    "reason_code",
    "note",
    "metadata_json",
]


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def _write_csv_rows(path: Path, headers: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: str(r.get(h, "")) for h in headers})


def _parse_int(v: str) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def recompute_tenants_credits(billing_state_dir: Path) -> None:
    """Recompute tenants_credits.csv deterministically from the append-only ledger.

    Policy:
    - transactions.csv is treated as the SoT for credit movements.
    - type=TOPUP increases credits_available
    - type=SPEND decreases credits_available
    - unknown types are ignored

    This recomputation is safe even if tenants_credits.csv was seeded incorrectly
    because ledger history is not rewritten.
    """

    billing_state_dir = billing_state_dir.resolve()
    transactions_path = billing_state_dir / "transactions.csv"
    tenants_credits_path = billing_state_dir / "tenants_credits.csv"

    transactions = _read_csv_rows(transactions_path)
    existing = _read_csv_rows(tenants_credits_path)

    status_by_tenant: Dict[str, str] = {}
    for r in existing:
        tid = str(r.get("tenant_id", "")).strip()
        if not tid:
            continue
        status_by_tenant[tid] = str(r.get("status", "active") or "active").strip() or "active"

    balance: Dict[str, int] = {}
    for tx in transactions:
        tid = str(tx.get("tenant_id", "")).strip()
        if not tid:
            continue
        ttype = str(tx.get("type", "")).strip().upper()
        amt = _parse_int(tx.get("amount_credits", "0"))

        if ttype == "TOPUP":
            balance[tid] = balance.get(tid, 0) + amt
        elif ttype == "SPEND":
            balance[tid] = balance.get(tid, 0) - amt
        elif ttype in ("REFUND", "CREDIT"):
            balance[tid] = balance.get(tid, 0) + amt
        elif ttype in ("DEBIT",):
            balance[tid] = balance.get(tid, 0) - amt
        else:
            continue

    now = utcnow_iso()
    rows: List[Dict[str, str]] = []
    for tid in sorted(balance.keys() | status_by_tenant.keys()):
        rows.append(
            {
                "tenant_id": tid,
                "credits_available": str(balance.get(tid, 0)),
                "updated_at": now,
                "status": status_by_tenant.get(tid, "active"),
            }
        )

    _write_csv_rows(tenants_credits_path, TENANTS_CREDITS_HEADERS, rows)
