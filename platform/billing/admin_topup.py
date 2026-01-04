from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from ..common.id_codec import canon_tenant_id, canon_topup_method_id, id_key
from ..common.id_policy import generate_unique_id
from ..utils.csvio import read_csv, write_csv
from ..utils.time import utcnow_iso
from .payments import PAYMENTS_CSV_HEADERS, validate_repo_payments


ADMIN_TOPUP_METHOD_NAME = "Admin Top Up"


@dataclass(frozen=True)
class AdminTopupResult:
    payment_id: str
    tenant_id: str
    topup_method_id: str
    amount_credits: int
    received_at: str
    reference: str


def _load_topup_methods(repo_root: Path) -> List[Dict[str, str]]:
    return read_csv(repo_root / "platform" / "billing" / "topup_instructions.csv")


def _resolve_admin_topup_method_id(repo_root: Path) -> str:
    """Return topup_method_id for the catalog row named 'Admin Top Up' (case-insensitive)."""
    rows = _load_topup_methods(repo_root)
    for r in rows:
        name = str(r.get("name", "")).strip()
        if name.lower() == ADMIN_TOPUP_METHOD_NAME.lower():
            tid = canon_topup_method_id(r.get("topup_method_id", ""))
            if not tid:
                raise ValueError("Admin Top Up method row has invalid topup_method_id")
            enabled = str(r.get("enabled", "")).strip().lower()
            if enabled == "false":
                raise ValueError("Admin Top Up method is disabled in topup_instructions.csv")
            return tid
    raise ValueError(
        "Admin Top Up payment method not found in platform/billing/topup_instructions.csv "
        f"(expected name '{ADMIN_TOPUP_METHOD_NAME}')"
    )


def append_admin_topup_payment(
    repo_root: Path,
    tenant_id: str,
    amount_credits: int,
    reference: str = "",
    note: str = "",
    status: str = "CONFIRMED",
) -> AdminTopupResult:
    """Append a CONFIRMED admin top-up record into platform/billing/payments.csv.

    This is the single source of truth for admin top-ups; credits are increased when
    reconcile-payments is executed against billing-state.
    """
    t = canon_tenant_id(tenant_id)
    if not t:
        raise ValueError("tenant_id is required")

    if amount_credits <= 0:
        raise ValueError("amount_credits must be a positive integer")

    status_u = (status or "").strip().upper() or "CONFIRMED"
    if status_u not in ("CONFIRMED", "SETTLED"):
        raise ValueError("status must be CONFIRMED or SETTLED")

    topup_method_id = _resolve_admin_topup_method_id(repo_root)

    payments_path = repo_root / "platform" / "billing" / "payments.csv"
    rows = read_csv(payments_path)

    used: Set[str] = {id_key(r.get("payment_id")) for r in rows if id_key(r.get("payment_id"))}
    pid = generate_unique_id("payment_id", used)

    received_at = utcnow_iso()
    ref = (reference or "").strip() or f"ADMIN_TOPUP:{pid}"
    note_s = (note or "").strip()

    rows.append(
        {
            "payment_id": pid,
            "tenant_id": t,
            "topup_method_id": topup_method_id,
            "amount_credits": str(int(amount_credits)),
            "reference": ref,
            "received_at": received_at,
            "status": status_u,
            "note": note_s,
        }
    )

    write_csv(payments_path, rows, PAYMENTS_CSV_HEADERS)

    # Validate after write to fail-fast in workflows.
    report = validate_repo_payments(repo_root)
    if report.errors:
        raise ValueError("Invalid payments.csv after admin top-up: " + "; ".join(report.errors))

    return AdminTopupResult(
        payment_id=pid,
        tenant_id=t,
        topup_method_id=topup_method_id,
        amount_credits=int(amount_credits),
        received_at=received_at,
        reference=ref,
    )
