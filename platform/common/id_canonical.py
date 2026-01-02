"""Canonical identifier formatting.

This repository enforces fixed-width, zero-padded identifiers in several places
(e.g., scripts/ci_verify.py):
  - tenant_id: 10 digits
  - module_id: 6 digits

Problem:
  CSV has no types; common tooling (Excel/pandas) can coerce digits-only ids to
  numbers and drop leading zeros (e.g., "0000000001" -> "1").

Approach:
  - For matching/joins you may use platform.common.id_normalize.normalize_id
    (digits-only -> no leading zeros).
  - For persistence and schema contracts, use the canonicalizers below, which
    re-pad digits-only ids back to the required width.

Non-digit ids (e.g., "wo-2025-12-31-001", "E001") are preserved (trimmed).
"""

from __future__ import annotations

from typing import Any

from .id_normalize import normalize_id


TENANT_ID_WIDTH = 10
MODULE_ID_WIDTH = 6


def canonical_tenant_id(value: Any) -> str:
    """Return tenant_id in canonical repo format (10 digits) when digits-only."""
    s = normalize_id(value)
    if s.isdigit():
        return s.zfill(TENANT_ID_WIDTH)
    return s


def canonical_module_id(value: Any) -> str:
    """Return module_id in canonical repo format (6 digits) when digits-only."""
    s = normalize_id(value)
    if s.isdigit():
        return s.zfill(MODULE_ID_WIDTH)
    return s

