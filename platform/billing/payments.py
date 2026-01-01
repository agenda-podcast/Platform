from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..utils.csvio import read_csv
from .state import BillingState
from .topup import TopupRequest, apply_admin_topup


@dataclass(frozen=True)
class PaymentRecord:
    payment_id: str
    tenant_id: str
    topup_method_id: str
    amount_credits: int
    reference: str
    received_at: str
    status: str
    note: str


def _load_payments(repo_root: Path) -> List[PaymentRecord]:
    path = repo_root / "platform" / "billing" / "payments.csv"
    if not path.exists():
        return []
    rows = read_csv(path)
    out: List[PaymentRecord] = []
    for r in rows:
        payment_id = str(r.get("payment_id", "")).strip()
        tenant_id = str(r.get("tenant_id", "")).strip()
        topup_method_id = str(r.get("topup_method_id", "")).strip()
        amt_raw = str(r.get("amount_credits", "")).strip()
        reference = str(r.get("reference", "")).strip()
        received_at = str(r.get("received_at", "")).strip()
        status = str(r.get("status", "")).strip()
        note = str(r.get("note", "")).strip()

        if not payment_id:
            # Ignore blank rows
            continue
        try:
            amount = int(amt_raw)
        except Exception:
            raise ValueError(f"Invalid amount_credits for payment_id={payment_id!r}: {amt_raw!r}")
        out.append(PaymentRecord(
            payment_id=payment_id,
            tenant_id=tenant_id,
            topup_method_id=topup_method_id,
            amount_credits=amount,
            reference=reference,
            received_at=received_at,
            status=status,
            note=note,
        ))
    return out


def reconcile_repo_payments_into_billing_state(repo_root: Path, billing: BillingState) -> Tuple[int, List[str]]:
    """Apply repo-recorded payments to billing-state.

    Goal:
      - Admin records actual payments received in platform/billing/payments.csv (repo-managed).
      - Maintenance run reconciles CONFIRMED payments into the billing-state ledger (Release SoT),
        increasing tenant credits without anyone manually editing Release assets.

    Idempotency:
      - Each payment produces a transaction_item.name = topup:<topup_method_id>:<payment_id>
      - If that name already exists in transaction_items.csv, the payment is considered already applied.

    Returns: (applied_count, applied_transaction_ids)
    """
    payments = _load_payments(repo_root)
    if not payments:
        return (0, [])

    # Load existing transaction items for idempotency checks
    transaction_items = billing.load_table("transaction_items.csv")
    existing_names = {str(r.get("name", "")).strip() for r in transaction_items if str(r.get("name", "")).strip()}

    applied_tx_ids: List[str] = []
    applied_count = 0

    for p in payments:
        status = p.status.strip().upper()
        if status not in ("CONFIRMED", "SETTLED"):
            continue

        if not p.tenant_id:
            raise ValueError(f"payment_id={p.payment_id!r} missing tenant_id")
        if not p.topup_method_id:
            raise ValueError(f"payment_id={p.payment_id!r} missing topup_method_id")
        if p.amount_credits <= 0:
            raise ValueError(f"payment_id={p.payment_id!r} amount_credits must be positive")

        name_reference = f"{p.payment_id}"
        item_name = f"topup:{p.topup_method_id}:{p.payment_id}"

        if item_name in existing_names:
            continue

        note_parts = []
        if p.reference:
            note_parts.append(f"reference={p.reference}")
        if p.received_at:
            note_parts.append(f"received_at={p.received_at}")
        if p.note:
            note_parts.append(p.note)
        note = "; ".join(note_parts).strip()

        tx_id = apply_admin_topup(
            repo_root=repo_root,
            billing=billing,
            req=TopupRequest(
                tenant_id=p.tenant_id,
                amount_credits=p.amount_credits,
                topup_method_id=p.topup_method_id,
                reference=name_reference,
                note=note,
            ),
        )

        # Overwrite the generated item name with our idempotent name convention
        transaction_items = billing.load_table("transaction_items.csv")
        # Find the latest item for this tx (ti-<tx>-0001) and set name deterministically
        for r in reversed(transaction_items):
            if str(r.get("transaction_id", "")) == tx_id:
                r["name"] = item_name
                break
        billing.save_table(
            "transaction_items.csv",
            transaction_items,
            ["transaction_item_id","transaction_id","tenant_id","work_order_id","module_run_id","name","category","amount_credits","reason_code","note"],
        )

        existing_names.add(item_name)
        applied_count += 1
        applied_tx_ids.append(tx_id)

    # Persist manifest only if anything changed
    if applied_count:
        billing.write_state_manifest()

    return (applied_count, applied_tx_ids)
