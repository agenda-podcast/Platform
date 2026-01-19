from __future__ import annotations

import re
import secrets
from typing import Dict, Optional, Set

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE62_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

ID_LENGTHS: Dict[str, int] = {
    "tenant_id": 6,
    "transaction_id": 8,
    "transaction_item_id": 10,
    "module_run_id": 10,
    "payment_id": 8,
    "topup_method_id": 3,
    "product_code": 3,
    "github_release_asset_id": 12,
}

_RE_CACHE: Dict[str, re.Pattern[str]] = {}

_MODULE_ID_BASE62_RE = re.compile(r"^[0-9A-Za-z]{2}[A-Za-z]$")
_MODULE_ID_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{1,62}[a-z]$")
_WORK_ORDER_ID_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_-]{2,63}[A-Za-z]$")
_STEP_ID_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z_-]{0,31}[A-Za-z]$")
_REASON_CODE_RE = re.compile(r"^(?:[0-9A-Za-z]{7}[A-Za-z]|[A-Z][A-Z0-9_]{0,63}[A-Z])$")
_REASON_KEY_RE = re.compile(r"^[A-Za-z0-9_]{1,32}[A-Za-z]$")

def id_length(id_type: str) -> int:
    if id_type not in ID_LENGTHS:
        raise ValueError(f"Unknown id_type: {id_type!r}")
    return int(ID_LENGTHS[id_type])

def id_regex(id_type: str) -> re.Pattern[str]:
    if id_type in _RE_CACHE:
        return _RE_CACHE[id_type]

    if id_type == "module_id":
        pat = re.compile(r"^(?:[0-9A-Za-z]{2}[A-Za-z]|[a-z][a-z0-9_]{1,62}[a-z])$")
        _RE_CACHE[id_type] = pat
        return pat

    if id_type == "work_order_id":
        _RE_CACHE[id_type] = _WORK_ORDER_ID_RE
        return _WORK_ORDER_ID_RE

    if id_type == "step_id":
        _RE_CACHE[id_type] = _STEP_ID_RE
        return _STEP_ID_RE

    if id_type == "reason_code":
        _RE_CACHE[id_type] = _REASON_CODE_RE
        return _REASON_CODE_RE

    if id_type == "reason_key":
        _RE_CACHE[id_type] = _REASON_KEY_RE
        return _REASON_KEY_RE

    n = id_length(id_type)
    if n < 2:
        raise ValueError(f"Invalid fixed length for {id_type!r}: {n}")
    pat = re.compile(rf"^[0-9A-Za-z]{{{n-1}}}[A-Za-z]$")
    _RE_CACHE[id_type] = pat
    return pat

def is_valid_id(id_type: str, value: str) -> bool:
    v = str(value or "").strip()
    if not v:
        return False
    return bool(id_regex(id_type).match(v))

def validate_id(id_type: str, value: str, field_name: str = "") -> None:
    v = str(value or "").strip()
    nm = field_name or id_type
    if not v:
        raise ValueError(f"Missing {nm}")
    if id_type == "module_id":
        if not (_MODULE_ID_BASE62_RE.match(v) or _MODULE_ID_SLUG_RE.match(v)):
            raise ValueError(f"Invalid {nm}: {value!r}")
        return
    if not is_valid_id(id_type, v):
        raise ValueError(f"Invalid {nm}: {value!r}")

def new_id(id_type: str, used: Optional[Set[str]] = None) -> str:
    used = used or set()
    if id_type in {"module_id", "work_order_id", "step_id", "reason_code", "reason_key"}:
        raise ValueError(f"new_id only supports fixed-length Base62 ids; got {id_type!r}")
    n = id_length(id_type)
    if n < 2:
        raise ValueError(f"Invalid fixed length for {id_type!r}: {n}")
    while True:
        prefix = "".join(secrets.choice(BASE62_ALPHABET) for _ in range(n - 1))
        last = secrets.choice(BASE62_LETTERS)
        out = prefix + last
        if out not in used:
            used.add(out)
            return out

def generate_id(id_type: str) -> str:
    return new_id(id_type, used=None)

def generate_unique_id(id_type: str, used: Optional[Set[str]] = None) -> str:
    return new_id(id_type, used)
