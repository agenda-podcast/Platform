"""Identifier normalization + canonicalization.

This repository uses fixed-width numeric identifiers:
  - tenant_id: 10 digits (0000000001 .. 9999999999)
  - module_id:  6 digits (000001 .. 999999)
  - reason_code: 12 digits (GCCMMMMMMRRR)

CSV tooling (notably Excel) may coerce these to numbers and drop leading zeros,
causing exact-string joins to fail.

Policy implemented here:
  - **Matching key**: digits-only values are compared by numeric value
    (i.e., leading zeros ignored): key("0001") == key("1") == "1".
  - **Canonical storage**: when we write accounting state, we write canonical
    fixed-width IDs to reduce drift.

This module is intentionally tiny and dependency-free so it can be used
everywhere (billing, orchestration, maintenance).
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple


_DIGITS_RE = re.compile(r"^\d+$")


def id_key(value: Any) -> str:
    """Return a deterministic key for joining identifiers.

    - None/blank -> ""
    - digits-only -> numeric string without leading zeros ("000" -> "0")
    - otherwise -> trimmed string unchanged
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if _DIGITS_RE.match(s):
        s2 = s.lstrip("0")
        return s2 if s2 else "0"
    return s


def canon_tenant_id(value: Any) -> str:
    """Canonical 10-digit tenant_id when the value is digits-only."""
    k = id_key(value)
    if not k or not _DIGITS_RE.match(k):
        return str(value).strip() if value is not None else ""
    # Guard: tenant_id must fit 10 digits and be >= 1 in this scaffold.
    n = int(k)
    if n <= 0 or n >= 10_000_000_000:
        return k
    return f"{n:010d}"


def canon_module_id(value: Any) -> str:
    """Canonical 6-digit module_id when the value is digits-only."""
    k = id_key(value)
    if not k or not _DIGITS_RE.match(k):
        return str(value).strip() if value is not None else ""
    n = int(k)
    if n <= 0 or n >= 1_000_000:
        return k
    return f"{n:06d}"


def canon_reason_code(value: Any) -> str:
    """Canonical 12-digit reason_code when digits-only.

    If Excel converts "001..." to "1...", this restores padding.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if _DIGITS_RE.match(s) and len(s) <= 12:
        return s.zfill(12)
    return s


def dedupe_tenants_credits(rows: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], int]:
    """Deterministically merge duplicate tenant rows caused by ID formatting drift.

    Merge key:
      id_key(tenant_id)

    Keep policy (deterministic):
      1) status active beats non-active
      2) higher credits_available
      3) newer updated_at (lexicographic ISO)
      4) later row position

    The kept row is rewritten with canonical tenant_id (10 digits).
    Returns (deduped_rows, dropped_count).
    """

    best: Dict[str, Tuple[int, Dict[str, str]]] = {}  # key -> (score_tuple_as_int_rank, row)
    dropped = 0

    def _status_rank(v: str) -> int:
        s = (v or "").strip().lower()
        return 1 if s == "active" else 0

    def _credits(v: str) -> int:
        try:
            return int(str(v or "0").strip() or 0)
        except Exception:
            return 0

    for idx, r0 in enumerate(rows):
        r = dict(r0)
        k = id_key(r.get("tenant_id", ""))
        if not k:
            continue
        st = _status_rank(str(r.get("status", "")))
        cr = _credits(str(r.get("credits_available", "0")))
        ts = str(r.get("updated_at", "") or "").strip()
        # Build a tuple that compares deterministically.
        score = (st, cr, ts, idx)

        if k not in best:
            best[k] = (score, r)
            continue

        prev_score, _ = best[k]
        if score > prev_score:
            best[k] = (score, r)
            dropped += 1
        else:
            dropped += 1

    # Preserve output deterministically by the last component (idx) in the chosen score
    ordered = sorted(best.items(), key=lambda kv: kv[1][0][3])
    out: List[Dict[str, str]] = []
    for k, (score, r) in ordered:
        r["tenant_id"] = canon_tenant_id(k)
        # normalize status casing
        if "status" in r and r["status"]:
            r["status"] = str(r["status"]).strip().lower()
        out.append(r)
    return out, dropped
