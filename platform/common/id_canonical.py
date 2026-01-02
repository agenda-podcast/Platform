"""Canonical identifier formatting.

Repo contract (validated by scripts/ci_verify.py):
- tenant_id: 10 digits, zero-padded (e.g. 0000000001)
- module_id: 6 digits, zero-padded (e.g. 000003)

Problem:
- CSV has no types. Excel/pandas frequently coerce digit-only strings to numbers and drop leading zeros.
- If matching is done as raw strings, lookups fail (tenant not found / not enough credits / price not found).

Solution:
- Canonicalize IDs at ingestion boundaries and before any join/lookup.
- For digit-only IDs, normalize to the contract width via zfill().
- For non-digit IDs (e.g. 'wo-2025-12-31-001', 'tx-...'), preserve as trimmed strings.

This module must be used anywhere IDs are compared or used as dictionary keys.
"""

from __future__ import annotations

from typing import Any


TENANT_ID_WIDTH = 10
MODULE_ID_WIDTH = 6


def _to_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def normalize_digits_zfill(value: Any, width: int) -> str:
    """Normalize digit-only strings to a fixed width with leading zeros."""
    s = _to_str(value)
    if not s:
        return ""
    if s.isdigit():
        # Keep '0' as all zeros of the required width
        return s.zfill(width)
    return s


def normalize_tenant_id(value: Any) -> str:
    return normalize_digits_zfill(value, TENANT_ID_WIDTH)


def normalize_module_id(value: Any) -> str:
    return normalize_digits_zfill(value, MODULE_ID_WIDTH)
