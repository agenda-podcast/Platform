from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..utils.csvio import read_csv, require_headers
from ..common.id_codec import canon_payment_id, canon_tenant_id, canon_topup_method_id, id_key
from ..common.id_policy import validate_id
from .state import BillingState
from .topup import TopupApplyRequest, apply_topup_ledger


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


def validate_repo_payments(repo_root: Path) -> PaymentsValidationReport:
    path = repo_root / "platform" / "billing" / "payments.csv"
    rows = read_csv(path)
    require_headers(path, PAYMENTS_CSV_HEADERS)

    errors: List[str] = []
    warnings: List[str] = []

    eligible = 0

    # Detect duplicates on (tenant_id, reference, amount, received_at) among eligible statuses
    eligible_dupe_key_to_ids: Dict[Tuple[str, str, str, str], List[str]] = {}

    for idx, r in enumerate(rows, start=2):  # header is line 1
        payment_id = canon_payment_id(r.get("payment_id", ""))
        tenant_id = canon_tenant_id(r.get("tenant_id", ""))
        topup_method_id = canon_topup_method_id(r.get("topup_method_id", ""))
        amount = str(r.get("amount_credits", "")).strip()
        reference = str(r.get("reference", "")).strip()
        received_at = str(r.get("received_at", "")).strip()
        status = str(r.get("status", "")).strip().upper()

        if not payment_id:
            errors.append(f"Row {idx}: invalid payment_id")
        if not tenant_id:
            errors.append(f"Row {idx}: invalid tenant_id")
        if not topup_method_id:
            errors.append(f"Row {idx}: invalid topup_method_id")

        try:
            av = int(amount)
            if av <= 0:
                raise ValueError()
        except Exception:
            errors.append(f"Row {idx}: invalid amount_credits (expected positive integer)")

        if status not in ("CONFIRMED", "SETTLED", "PENDING", "REJECTED", "CANCELLED", ""):
            warnings.append(f"Row {idx}: unexpected status {status!r}")

        if status in ("CONFIRMED", "SETTLED"):
            eligible += 1
            key = (tenant_id, reference, amount, received_at)
            eligible_dupe_key_to_ids.setdefault(key, []).append(payment_id)

    for key, ids in eligible_dupe_key_to_ids.items():
        if len(ids) > 1:
            tenant_id, reference, amount, received_at = key
            errors.append(
                "Eligible payments duplicate detected: "
                f"tenant_id={tenant_id!r}, reference={reference!r}, amount_credits={amount!r}, received_at={received_at!r}; payment_ids={ids}"
            )

    return PaymentsValidationReport(payments_seen=len(rows), eligible_seen=eligible, errors=errors, warnings=warnings)


def reconcile_repo_payments_into_billing_state(repo_root: Path, billing: BillingState) -> PaymentsReconcileResult:
    report = validate_repo_payments(repo_root)
    if report.errors:
        msg = "Invalid payments.csv. Fix the following issues:\n" + "\n".join(f"- {e}" for e in report.errors)
        raise ValueError(msg)

    path = repo_root / "platform" / "billing" / "payments.csv"
    rows = read_csv(path)

    # Identify already applied payments via transactions metadata_json
    transactions = billing.load_table("transactions.csv")
    applied_payment_ids = set()
    for t in transactions:
        meta = str(t.get("metadata_json", "")).strip()
        if not meta:
            continue
        try:
            obj = json.loads(meta)
        except Exception:
            continue
        pid = str(obj.get("payment_id", "")).strip()
        if pid:
            applied_payment_ids.add(pid)

    applied_tx_ids: List[str] = []
    applied = 0
    skipped = 0
    eligible = 0

    for r in rows:
        status = str(r.get("status", "")).strip().upper()
        if status not in ("CONFIRMED", "SETTLED"):
            continue
        eligible += 1

        payment_id = canon_payment_id(r.get("payment_id", ""))
        if payment_id in applied_payment_ids:
            skipped += 1
            continue

        tenant_id = canon_tenant_id(r.get("tenant_id", ""))
        topup_method_id = canon_topup_method_id(r.get("topup_method_id", ""))
        amount = int(str(r.get("amount_credits", "")).strip())
        reference = str(r.get("reference", "")).strip()
        note = str(r.get("note", "")).strip()

        tx_id = apply_topup_ledger(
            repo_root=repo_root,
            billing=billing,
            req=TopupApplyRequest(
                tenant_id=tenant_id,
                topup_method_id=topup_method_id,
                amount_credits=amount,
                reference=reference,
                note=note,
            ),
        )

        # Patch the topup transaction metadata to include payment_id as idempotency marker
        tx_rows = billing.load_table("transactions.csv")
        for t in tx_rows:
            if id_key(t.get("transaction_id")) == tx_id:
                meta = {}
                try:
                    meta = json.loads(str(t.get("metadata_json", "")).strip() or "{}")
                except Exception:
                    meta = {}
                meta["payment_id"] = payment_id
                t["metadata_json"] = json.dumps(meta, separators=(",", ":"))
                break
        billing.save_table(
            "transactions.csv",
            tx_rows,
            ["transaction_id","tenant_id","work_order_id","type","amount_credits","created_at","reason_code","note","metadata_json"],
        )

        applied += 1
        applied_tx_ids.append(tx_id)

    return PaymentsReconcileResult(
        payments_seen=report.payments_seen,
        payments_eligible=eligible,
        payments_applied=applied,
        payments_skipped_already_applied=skipped,
        applied_transaction_ids=applied_tx_ids,
    )
