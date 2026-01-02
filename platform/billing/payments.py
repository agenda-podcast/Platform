from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from ..utils.csvio import read_csv, require_headers
from ..common.id_canonical import canonical_tenant_id
from ..common.id_normalize import normalize_id
from .state import BillingState
from .topup import TopupRequest, apply_admin_topup


PAYMENTS_CSV_HEADERS: List[str] = [
    "payment_id",
    "tenant_id",
    "topup_method_id",
    "amount_credits",
    "reference",
    "received_at",
    "status",
    "note",
]


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


@dataclass(frozen=True)
class PaymentsValidationReport:
    payments_seen: int
    eligible_seen: int
    errors: List[str]
    warnings: List[str]


@dataclass(frozen=True)
class PaymentsReconcileResult:
    payments_seen: int
    payments_eligible: int
    payments_applied: int
    payments_skipped_already_applied: int
    applied_transaction_ids: List[str]


def _payments_csv_path(repo_root: Path) -> Path:
    return repo_root / "platform" / "billing" / "payments.csv"


def _load_active_topup_method_ids(repo_root: Path) -> Dict[str, Dict[str, str]]:
    """Return topup_method_id -> row for active methods only."""
    cfg = repo_root / "platform" / "billing" / "topup_instructions.csv"
    rows = read_csv(cfg)
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = str(r.get("topup_method_id", "")).strip()
        if not mid:
            continue
        status = str(r.get("status", "")).strip().lower()
        if status == "active":
            out[mid] = r
    return out


def _coerce_int(val: str, *, context: str) -> int:
    try:
        return int(str(val).strip())
    except Exception:
        raise ValueError(f"Invalid integer for {context}: {val!r}")


def _is_blank_row(row: Dict[str, str], keys: Sequence[str]) -> bool:
    return all(not str(row.get(k, "")).strip() for k in keys)


def load_repo_payments(repo_root: Path) -> List[PaymentRecord]:
    """Load payments.csv into strongly-typed records.

    This function does **not** validate business rules beyond parsing. Call
    validate_repo_payments() before reconciliation.
    """
    path = _payments_csv_path(repo_root)
    if not path.exists():
        return []

    rows = read_csv(path)
    out: List[PaymentRecord] = []
    for r in rows:
        if _is_blank_row(r, PAYMENTS_CSV_HEADERS):
            continue

        payment_id = str(r.get("payment_id", "")).strip()
        tenant_id = canonical_tenant_id(r.get("tenant_id", ""))
        topup_method_id = str(r.get("topup_method_id", "")).strip()
        amount_credits = _coerce_int(str(r.get("amount_credits", "")), context=f"payment_id={payment_id or '<missing>'}/amount_credits")
        reference = str(r.get("reference", "")).strip()
        received_at = str(r.get("received_at", "")).strip()
        status = str(r.get("status", "")).strip()
        note = str(r.get("note", "")).strip()

        out.append(
            PaymentRecord(
                payment_id=payment_id,
                tenant_id=tenant_id,
                topup_method_id=topup_method_id,
                amount_credits=amount_credits,
                reference=reference,
                received_at=received_at,
                status=status,
                note=note,
            )
        )
    return out


def validate_repo_payments(repo_root: Path) -> PaymentsValidationReport:
    """Validate platform/billing/payments.csv and return a report.

    Hard failures raise ValueError with a multi-line message listing all invalid rows.
    """
    path = _payments_csv_path(repo_root)
    if not path.exists():
        return PaymentsValidationReport(payments_seen=0, eligible_seen=0, errors=[], warnings=[])

    # Schema check (headers)
    require_headers(path, PAYMENTS_CSV_HEADERS)

    active_methods = _load_active_topup_method_ids(repo_root)

    rows = read_csv(path)
    errors: List[str] = []
    warnings: List[str] = []
    seen_payment_ids: Dict[str, int] = {}
    seen: int = 0
    eligible: int = 0

    # Duplicate guardrail for eligible payments
    eligible_key_to_payment_ids: Dict[Tuple[str, str, int, str], List[str]] = {}

    allowed_statuses = {"PENDING", "RECEIVED", "CONFIRMED", "SETTLED", "REJECTED", "CANCELLED"}

    for i, r in enumerate(rows, start=2):  # 1-based header line
        if _is_blank_row(r, PAYMENTS_CSV_HEADERS):
            continue

        payment_id = str(r.get("payment_id", "")).strip()
        tenant_id = canonical_tenant_id(r.get("tenant_id", ""))
        topup_method_id = str(r.get("topup_method_id", "")).strip()
        amount_raw = str(r.get("amount_credits", "")).strip()
        reference = str(r.get("reference", "")).strip()
        received_at = str(r.get("received_at", "")).strip()
        status_raw = str(r.get("status", "")).strip()
        status = status_raw.upper()

        if not payment_id:
            errors.append(f"L{i}: payment_id is required for non-blank rows")
            continue

        if payment_id in seen_payment_ids:
            errors.append(f"L{i}: duplicate payment_id={payment_id!r} (already seen at L{seen_payment_ids[payment_id]})")
        else:
            seen_payment_ids[payment_id] = i

        seen += 1

        if not tenant_id:
            errors.append(f"L{i}: payment_id={payment_id!r} missing tenant_id")

        if not topup_method_id:
            errors.append(f"L{i}: payment_id={payment_id!r} missing topup_method_id")
        elif topup_method_id not in active_methods:
            errors.append(
                f"L{i}: payment_id={payment_id!r} unknown or non-active topup_method_id={topup_method_id!r} (must exist in platform/billing/topup_instructions.csv with status=active)"
            )

        try:
            amount = int(amount_raw)
        except Exception:
            errors.append(f"L{i}: payment_id={payment_id!r} amount_credits is not an integer: {amount_raw!r}")
            amount = 0
        if amount <= 0:
            errors.append(f"L{i}: payment_id={payment_id!r} amount_credits must be > 0")

        if not status:
            errors.append(f"L{i}: payment_id={payment_id!r} status is required")
        elif status not in allowed_statuses:
            warnings.append(f"L{i}: payment_id={payment_id!r} status={status_raw!r} is not in known statuses {sorted(allowed_statuses)}")

        is_eligible = status in ("CONFIRMED", "SETTLED")
        if is_eligible:
            eligible += 1
            # Duplicate detection key among eligible payments
            key = (normalize_id(tenant_id), reference, amount, received_at)
            eligible_key_to_payment_ids.setdefault(key, []).append(payment_id)

    # Duplicate eligible payments guardrail
    for key, pids in eligible_key_to_payment_ids.items():
        if len(pids) > 1:
            tenant_id, reference, amount, received_at = key
            errors.append(
                "Eligible payments duplicate detected: "
                f"tenant_id={tenant_id!r}, reference={reference!r}, amount_credits={amount!r}, received_at={received_at!r}; payment_ids={pids}"
            )

    if errors:
        msg = "Invalid payments.csv. Fix the following issues:\n" + "\n".join(f"- {e}" for e in errors)
        raise ValueError(msg)

    return PaymentsValidationReport(payments_seen=seen, eligible_seen=eligible, errors=[], warnings=warnings)


def reconcile_repo_payments_into_billing_state(repo_root: Path, billing: BillingState) -> PaymentsReconcileResult:
    """Apply repo-recorded payments to billing-state.

    Workflow contract:
      - Admin records payments in platform/billing/payments.csv (repo-managed).
      - Maintenance runs reconciliation, which appends TOPUP ledger entries and increases
        tenants_credits.csv in the billing-state Release asset pack (SoT).

    Idempotency:
      - Each payment must map to a deterministic transaction_item.name:
          topup:<topup_method_id>:<payment_id>
      - If that name exists in billing-state/transaction_items.csv, the payment is skipped.
    """

    # Fail-fast validation with a concrete row-level error list
    validate_repo_payments(repo_root)

    payments = load_repo_payments(repo_root)
    if not payments:
        return PaymentsReconcileResult(
            payments_seen=0,
            payments_eligible=0,
            payments_applied=0,
            payments_skipped_already_applied=0,
            applied_transaction_ids=[],
        )

    # Load existing transaction items for idempotency checks
    transaction_items = billing.load_table("transaction_items.csv")
    existing_names = {str(r.get("name", "")).strip() for r in transaction_items if str(r.get("name", "")).strip()}

    applied_tx_ids: List[str] = []
    applied: int = 0
    skipped_already_applied: int = 0
    eligible_seen: int = 0

    for p in payments:
        status = p.status.strip().upper()
        if status not in ("CONFIRMED", "SETTLED"):
            continue

        eligible_seen += 1

        item_name = f"topup:{p.topup_method_id}:{p.payment_id}"
        if item_name in existing_names:
            skipped_already_applied += 1
            continue

        note_parts: List[str] = []
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
                reference=p.payment_id,
                note=note,
            ),
        )

        # Overwrite the generated item name with our idempotent name convention
        transaction_items = billing.load_table("transaction_items.csv")
        for r in reversed(transaction_items):
            if str(r.get("transaction_id", "")) == tx_id:
                r["name"] = item_name
                break
        billing.save_table(
            "transaction_items.csv",
            transaction_items,
            [
                "transaction_item_id",
                "transaction_id",
                "tenant_id",
                "work_order_id",
                "module_run_id",
                "name",
                "category",
                "amount_credits",
                "reason_code",
                "note",
            ],
        )

        existing_names.add(item_name)
        applied += 1
        applied_tx_ids.append(tx_id)

    if applied:
        billing.write_state_manifest()

    return PaymentsReconcileResult(
        payments_seen=len(payments),
        payments_eligible=eligible_seen,
        payments_applied=applied,
        payments_skipped_already_applied=skipped_already_applied,
        applied_transaction_ids=applied_tx_ids,
    )
