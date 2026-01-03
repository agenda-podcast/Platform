from __future__ import annotations

from typing import Any, Dict, List

from .id_policy import validate_id

def id_key(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

def canon_tenant_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("tenant_id", v, "tenant_id")
    return v

def canon_work_order_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("work_order_id", v, "work_order_id")
    return v

def canon_module_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("module_id", v, "module_id")
    return v

def canon_transaction_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("transaction_id", v, "transaction_id")
    return v

def canon_transaction_item_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("transaction_item_id", v, "transaction_item_id")
    return v

def canon_module_run_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("module_run_id", v, "module_run_id")
    return v

def canon_reason_code(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("reason_code", v, "reason_code")
    return v

def canon_reason_key(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("reason_key", v, "reason_key")
    return v

def canon_payment_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("payment_id", v, "payment_id")
    return v

def canon_topup_method_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("topup_method_id", v, "topup_method_id")
    return v

def canon_product_code(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("product_code", v, "product_code")
    return v

def canon_github_release_asset_id(value: Any) -> str:
    v = id_key(value)
    if not v:
        return ""
    validate_id("github_release_asset_id", v, "github_release_asset_id")
    return v

def dedupe_tenants_credits(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_tid: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        tid = canon_tenant_id(r.get("tenant_id", ""))
        if not tid:
            continue
        prev = by_tid.get(tid)
        if prev is None or str(r.get("updated_at", "")) >= str(prev.get("updated_at", "")):
            by_tid[tid] = r
    return list(by_tid.values())
