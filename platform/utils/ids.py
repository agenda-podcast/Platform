from __future__ import annotations

import re
from dataclasses import dataclass

from ..common.id_policy import validate_id

CATEGORY_ID_RE = re.compile(r"^[0-9]{2}$")

def validate_category_id(category_id: str) -> None:
    v = str(category_id).strip()
    if not CATEGORY_ID_RE.match(v):
        raise ValueError(f"Invalid category_id: {category_id!r} (expected 2 digits)")

def validate_tenant_id(tenant_id: str) -> None:
    validate_id("tenant_id", tenant_id, "tenant_id")

def validate_work_order_id(work_order_id: str) -> None:
    validate_id("work_order_id", work_order_id, "work_order_id")

def validate_module_id(module_id: str) -> None:
    validate_id("module_id", module_id, "module_id")

def validate_transaction_id(transaction_id: str) -> None:
    validate_id("transaction_id", transaction_id, "transaction_id")

def validate_transaction_item_id(transaction_item_id: str) -> None:
    validate_id("transaction_item_id", transaction_item_id, "transaction_item_id")

def validate_module_run_id(module_run_id: str) -> None:
    validate_id("module_run_id", module_run_id, "module_run_id")

def validate_reason_code(reason_code: str) -> None:
    validate_id("reason_code", reason_code, "reason_code")

def validate_reason_key(reason_key: str) -> None:
    validate_id("reason_key", reason_key, "reason_key")

def validate_payment_id(payment_id: str) -> None:
    validate_id("payment_id", payment_id, "payment_id")

def validate_topup_method_id(topup_method_id: str) -> None:
    validate_id("topup_method_id", topup_method_id, "topup_method_id")

def validate_product_code(product_code: str) -> None:
    validate_id("product_code", product_code, "product_code")

def validate_github_release_asset_id(value: str, field_name: str = "github_release_asset_id") -> None:
    validate_id("github_release_asset_id", value, field_name)

@dataclass(frozen=True)
class ParsedReason:
    reason_code: str
    reason_slug: str
    scope: str
    module_id: str
