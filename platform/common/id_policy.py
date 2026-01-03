from __future__ import annotations

import re
import secrets
from typing import Dict, Optional, Set

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

ID_LENGTHS: Dict[str, int] = {
  "tenant_id": 6,
  "work_order_id": 8,
  "module_id": 3,
  "transaction_id": 8,
  "transaction_item_id": 8,
  "module_run_id": 8,
  "reason_code": 6,
  "reason_key": 3,
  "payment_id": 8,
  "topup_method_id": 2,
  "product_code": 3,
  "github_release_asset_id": 8
}

_REGEX_CACHE: Dict[str, re.Pattern] = {}

def id_length(id_type: str) -> int:
    if id_type not in ID_LENGTHS:
        raise KeyError(f"Unknown id_type: {id_type}")
    return ID_LENGTHS[id_type]

def id_regex(id_type: str) -> re.Pattern:
    if id_type in _REGEX_CACHE:
        return _REGEX_CACHE[id_type]
    n = id_length(id_type)
    pat = re.compile(rf"^[0-9A-Za-z]{{{n}}}$")
    _REGEX_CACHE[id_type] = pat
    return pat

def is_valid_id(id_type: str, value: Optional[str], allow_empty: bool = False) -> bool:
    if value is None:
        return allow_empty
    v = str(value).strip()
    if v == "":
        return allow_empty
    return bool(id_regex(id_type).match(v))

def validate_id(id_type: str, value: Optional[str], field_name: str = "", allow_empty: bool = False) -> None:
    if not is_valid_id(id_type, value, allow_empty=allow_empty):
        n = id_length(id_type)
        label = field_name or id_type
        raise ValueError(f"Invalid {label}: {value!r} (expected Base62 length {n})")

def generate_id(id_type: str) -> str:
    n = id_length(id_type)
    return "".join(secrets.choice(BASE62_ALPHABET) for _ in range(n))

def generate_unique_id(id_type: str, used: Set[str]) -> str:
    while True:
        v = generate_id(id_type)
        if v not in used:
            used.add(v)
            return v
