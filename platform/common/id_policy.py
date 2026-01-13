from __future__ import annotations

import re
import secrets
from typing import Dict, Optional, Set

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

# Canonical fixed-length Base62 IDs for core entities.
#
# Note: module_id historically was 3-char Base62. The platform now also allows
# stable, filesystem-friendly slugs for system modules (example: package_std).
# The ID length table is retained for legacy Base62 generation utilities.
ID_LENGTHS: Dict[str, int] = {
    "tenant_id": 6,
    "work_order_id": 8,
    "step_id": 2,
    "module_id": 3,
    "transaction_id": 8,
    "transaction_item_id": 10,
    "module_run_id": 10,
    "reason_code": 8,
    "reason_key": 3,
    "payment_id": 8,
    "topup_method_id": 3,
    "product_code": 3,
    "github_release_asset_id": 12,
}

# Compiled regex cache.
_RE_CACHE: Dict[str, re.Pattern[str]] = {}

# Legacy Base62 (fixed length) for module IDs.
_MODULE_ID_BASE62_RE = re.compile(r"^[0-9A-Za-z]{3}$")

# Slug module IDs: lowercase, underscores allowed, must start with a letter.
# Keep within 3..64 chars for UX and filesystem sanity.
_MODULE_ID_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


def id_length(id_type: str) -> int:
    if id_type not in ID_LENGTHS:
        raise ValueError(f"Unknown id_type: {id_type!r}")
    return int(ID_LENGTHS[id_type])


def id_regex(id_type: str) -> re.Pattern[str]:
    """Return the canonical validation regex for an id_type."""
    if id_type in _RE_CACHE:
        return _RE_CACHE[id_type]

    if id_type == "module_id":
        # Allow either legacy 3-char Base62 or a slug.
        pat = re.compile(r"^(?:[0-9A-Za-z]{3}|[a-z][a-z0-9_]{2,63})$")
        _RE_CACHE[id_type] = pat
        return pat

    n = id_length(id_type)
    pat = re.compile(rf"^[0-9A-Za-z]{{{n}}}$")
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
            raise ValueError(
                f"Invalid {nm}: {value!r} (expected legacy Base62 length 3 or slug pattern [a-z][a-z0-9_]{{2,63}})"
            )
        return

    n = id_length(id_type)
    if not is_valid_id(id_type, v):
        raise ValueError(f"Invalid {nm}: {value!r} (expected Base62 length {n})")


def new_id(id_type: str, used: Optional[Set[str]] = None) -> str:
    """Generate a new fixed-length Base62 ID (legacy).

    This is intended for id types with fixed Base62 lengths. For module_id, the
    platform also supports slug IDs but generation remains Base62.
    """
    used = used or set()
    n = id_length(id_type)
    while True:
        out = "".join(secrets.choice(BASE62_ALPHABET) for _ in range(n))
        if out not in used:
            used.add(out)
            return out




def generate_id(id_type: str) -> str:
    """Back-compat wrapper used by adapters to generate a new Base62 ID."""
    return new_id(id_type, used=None)


def generate_unique_id(id_type: str, used: Optional[Set[str]] = None) -> str:
    """Back-compat wrapper used across the repository."""
    return new_id(id_type, used)
