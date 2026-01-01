from __future__ import annotations

import re
from dataclasses import dataclass

MODULE_ID_RE = re.compile(r"^[0-9]{6}$")
CATEGORY_ID_RE = re.compile(r"^[0-9]{2}$")
REASON_ID_RE = re.compile(r"^[0-9]{3}$")


def validate_module_id(module_id: str) -> None:
    if not MODULE_ID_RE.match(module_id) or module_id == "000000":
        raise ValueError(f"Invalid module_id: {module_id!r} (expected 000001-999999)")


def validate_category_id(category_id: str) -> None:
    if not CATEGORY_ID_RE.match(category_id) or category_id == "00":
        raise ValueError(f"Invalid category_id: {category_id!r} (expected 01-99)")


def validate_reason_id(reason_id: str) -> None:
    if not REASON_ID_RE.match(reason_id) or reason_id == "000":
        raise ValueError(f"Invalid reason_id: {reason_id!r} (expected 001-999)")


def reason_code(g: int, category_id: str, module_id: str, reason_id: str) -> str:
    """Compose reason code GCCMMMMMMRRR (12 digits).

    - g: global flag (0|1)
    - CC: 2-digit category
    - MMMMMM: 6-digit module_id (or 000000 for global reasons)
    - RRR: 3-digit reason_id
    """
    if g not in (0, 1):
        raise ValueError(f"g must be 0|1, got {g}")
    validate_category_id(category_id)
    if module_id != "000000":
        validate_module_id(module_id)
    validate_reason_id(reason_id)
    return f"{g}{category_id}{module_id}{reason_id}"


@dataclass(frozen=True)
class ParsedReasonCode:
    reason_code: str
    g: int
    category_id: str
    module_id: str
    reason_id: str


def parse_reason_code(code: str) -> ParsedReasonCode:
    if not re.match(r"^[0-9]{12}$", code):
        raise ValueError(f"Invalid reason_code: {code!r} (expected 12 digits)")
    g = int(code[0])
    cat = code[1:3]
    mod = code[3:9]
    rid = code[9:12]
    if g not in (0, 1):
        raise ValueError(f"Invalid g in reason_code: {code}")
    validate_category_id(cat)
    if mod != "000000":
        validate_module_id(mod)
    validate_reason_id(rid)
    return ParsedReasonCode(code, g, cat, mod, rid)
