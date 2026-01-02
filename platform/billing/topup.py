from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from ..utils.csvio import read_csv
from ..utils.time import utcnow_iso
from .state import BillingState
from ..common.id_codec import canon_tenant_id, dedupe_tenants_credits, id_key


@dataclass(frozen=True)
class TopupRequest:
    tenant_id: str
    amount_credits: int
    topup_method_id: str
    reference: str
    note: str = ""


def _new_id(prefix: str) -> str:
    import uuid

    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _load_topup_methods(repo_root: Path) -> Dict[str, Dict[str, str]]:
    cfg = repo_root / "platform" / "billing" / "topup_instructions.csv"
    rows = read_csv(cfg)
    return {str(r.get("topup_method_id", "")).strip(): r for r in rows if r.get("topup_method_id")}


def apply_admin_topup(repo_root: Path, billing: BillingState, req: TopupRequest) -> str:
    """Apply a manual top-up by appending ledger entries and updating balance.

    Release assets remain the accounting system of record; this function updates the
    downloaded billing-state directory so it can be uploaded back to the Release.

    Returns transaction_id.
    """

    tenant_id = canon_tenant_id(req.tenant_id)
    if not tenant_id:
        raise ValueError("tenant_id is required")

    if req.amount_credits <= 0:
        raise ValueError("amount_credits must be a positive integer")

    topup_methods = _load_topup_methods(repo_root)
    method = topup_methods.get(req.topup_method_id.strip())
    if not method:
        raise ValueError(f"Unknown topup_method_id: {req.topup_method_id!r}")

    status = str(method.get("status", "")).strip().lower()
    if status and status not in ("active", "planned", "disabled"):
        raise ValueError(f"Invalid status for topup_method_id {req.topup_method_id!r}: {status!r}")
    if status == "disabled":
        raise ValueError(f"topup_method_id is disabled: {req.topup_method_id!r}")

    # Load billing-state tables
    tenants_credits = billing.load_table("tenants_credits.csv")
    # Repair drift + merge duplicates before mutation.
    tenants_credits, _dropped = dedupe_tenants_credits(tenants_credits)
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")

    # Ensure tenant row
    trow = None
    want = id_key(tenant_id)
    for r in tenants_credits:
        if id_key(r.get("tenant_id", "")) == want:
            trow = r
            # Canonicalize storage
            r["tenant_id"] = tenant_id
            break
    if trow is None:
        trow = {"tenant_id": tenant_id, "credits_available": "0", "updated_at": utcnow_iso(), "status": "active"}
        tenants_credits.append(trow)

    current = int(str(trow.get("credits_available", "0")) or 0)
    new_balance = current + int(req.amount_credits)

    # Append ledger
    tx_id = _new_id("tx")
    transactions.append({
        "transaction_id": tx_id,
        "tenant_id": tenant_id,
        "work_order_id": "",  # top-ups are admin operations
        "type": "TOPUP",
        "total_amount_credits": str(int(req.amount_credits)),
        "created_at": utcnow_iso(),
        "metadata_json": json.dumps({
            "topup_method_id": req.topup_method_id,
            "reference": req.reference,
            "note": req.note,
        }, sort_keys=True),
    })

    name = f"topup:{req.topup_method_id}:{req.reference}".strip(":")
    transaction_items.append({
        "transaction_item_id": f"ti-{tx_id}-0001",
        "transaction_id": tx_id,
        "tenant_id": tenant_id,
        "work_order_id": "",
        "module_run_id": "",
        "name": name,
        "category": "TOPUP",
        "amount_credits": str(int(req.amount_credits)),
        "reason_code": "",
        "note": (req.note or ""),
    })

    # Update balance
    trow["credits_available"] = str(new_balance)
    trow["updated_at"] = utcnow_iso()

    # Persist
    billing.save_table("tenants_credits.csv", tenants_credits, ["tenant_id","credits_available","updated_at","status"])
    billing.save_table("transactions.csv", transactions, ["transaction_id","tenant_id","work_order_id","type","total_amount_credits","created_at","metadata_json"])
    billing.save_table("transaction_items.csv", transaction_items, ["transaction_item_id","transaction_id","tenant_id","work_order_id","module_run_id","name","category","amount_credits","reason_code","note"])

    return tx_id
