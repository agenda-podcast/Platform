from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Set

from ..common.id_codec import canon_tenant_id, canon_topup_method_id, id_key, dedupe_tenants_credits
from ..common.id_policy import generate_unique_id
from ..utils.csvio import read_csv
from ..utils.time import utcnow_iso
from .state import BillingState


@dataclass(frozen=True)
class TopupApplyRequest:
    tenant_id: str
    topup_method_id: str
    amount_credits: int
    reference: str = ""
    note: Optional[str] = None


def _new_id(id_type: str, used: Set[str]) -> str:
    return generate_unique_id(id_type, used)


def _load_topup_methods(repo_root: Path) -> Dict[str, Dict[str, str]]:
    cfg = repo_root / "platform" / "billing" / "topup_instructions.csv"
    rows = read_csv(cfg)
    return {str(r.get("topup_method_id", "")).strip(): r for r in rows if r.get("topup_method_id")}


def apply_topup_ledger(repo_root: Path, billing: BillingState, req: TopupApplyRequest) -> str:
    """Apply a top-up by appending ledger entries and updating balance.

    Billing-state release assets remain the accounting system of record; this function updates the
    local billing-state tables which are later uploaded back to the billing-state release.
    """
    tenant_id = canon_tenant_id(req.tenant_id)
    if not tenant_id:
        raise ValueError("tenant_id is required")

    topup_method_id = canon_topup_method_id(req.topup_method_id)
    if not topup_method_id:
        raise ValueError("topup_method_id is required")

    if req.amount_credits <= 0:
        raise ValueError("amount_credits must be a positive integer")

    topup_methods = _load_topup_methods(repo_root)
    method = topup_methods.get(topup_method_id)
    if not method:
        raise ValueError(f"Unknown topup_method_id: {topup_method_id!r}")

    enabled = str(method.get("enabled", "")).strip().lower()
    if enabled and enabled not in ("true", "false"):
        raise ValueError(f"Invalid enabled for topup_method_id {topup_method_id!r}: {enabled!r}")
    if enabled == "false":
        raise ValueError(f"topup_method_id is disabled: {topup_method_id!r}")

    tenants_credits = dedupe_tenants_credits(billing.load_table("tenants_credits.csv"))
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")

    used_tx: Set[str] = {id_key(r.get("transaction_id")) for r in transactions if id_key(r.get("transaction_id"))}
    used_ti: Set[str] = {id_key(r.get("transaction_item_id")) for r in transaction_items if id_key(r.get("transaction_item_id"))}

    # Ensure tenant exists
    trow = None
    for r in tenants_credits:
        if canon_tenant_id(r.get("tenant_id")) == tenant_id:
            trow = r
            break
    if not trow:
        trow = {"tenant_id": tenant_id, "credits_available": "0", "updated_at": utcnow_iso(), "status": "ACTIVE"}
        tenants_credits.append(trow)

    try:
        current = int(str(trow.get("credits_available", "0")).strip() or "0")
    except Exception as e:
        raise ValueError(f"Invalid credits_available for tenant_id {tenant_id}: {trow.get('credits_available')!r}") from e

    tx_id = _new_id("transaction_id", used_tx)
    ti_id = _new_id("transaction_item_id", used_ti)

    meta = {"topup_method_id": topup_method_id}
    if req.reference:
        meta["reference"] = str(req.reference)

    transactions.append({
        "transaction_id": tx_id,
        "tenant_id": tenant_id,
        "work_order_id": "",
        "type": "TOPUP",
        "amount_credits": str(int(req.amount_credits)),
        "created_at": utcnow_iso(),
        "reason_code": "",
        "note": (req.note or ""),
        "metadata_json": json.dumps(meta, separators=(",", ":")),
    })

    transaction_items.append({
        "transaction_item_id": ti_id,
        "transaction_id": tx_id,
        "tenant_id": tenant_id,
        "module_id": "",
        "feature": "TOPUP",
        "type": "TOPUP",
        "amount_credits": str(int(req.amount_credits)),
        "created_at": utcnow_iso(),
        "note": (req.note or ""),
        "metadata_json": json.dumps(meta, separators=(",", ":")),
    })

    new_balance = current + int(req.amount_credits)
    trow["credits_available"] = str(new_balance)
    trow["updated_at"] = utcnow_iso()

    billing.save_table("tenants_credits.csv", tenants_credits, ["tenant_id","credits_available","updated_at","status"])
    billing.save_table("transactions.csv", transactions, ["transaction_id","tenant_id","work_order_id","type","amount_credits","created_at","reason_code","note","metadata_json"])
    billing.save_table("transaction_items.csv", transaction_items, ["transaction_item_id","transaction_id","tenant_id","module_id","feature","type","amount_credits","created_at","note","metadata_json"])

    return tx_id
