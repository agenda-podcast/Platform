"""ID normalization utilities.

Goal:
- Treat identifiers as *identifiers*, not numbers.
- Ensure joins and lookups are stable even if upstream tooling strips leading zeros.

Normalization rule (default):
- If value is digits-only (e.g., "000123"), normalize to canonical numeric string ("123").
- Preserve "0" ("000" -> "0").
- If value contains any non-digit characters, preserve as-is (after strip).

This supports a mixed future where some IDs become prefixed (e.g., "t0001", "wo-42").
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping, Optional

_DIGITS_RE = re.compile(r"^\d+$")


def canonicalize_digits(value: Any, width: int) -> str:
    """Canonicalize a digits-only identifier to a fixed-width, zero-padded string.

    Use this when you need a stable canonical representation (repo contract), while
    still accepting inputs that may have lost leading zeros (e.g., Excel).

    - digits-only input is treated as numeric and padded to `width`
    - non-digits are preserved (after strip)

    Examples (width=10):
      - "0000000001" -> "0000000001"
      - "1" -> "0000000001"
      - 1 -> "0000000001"
    """
    if value is None:
        return ""
    s = str(value).strip()
    if s == "":
        return ""
    if _DIGITS_RE.match(s):
        # strip then pad; avoids "000" -> "" edge case
        s2 = s.lstrip("0")
        s2 = s2 if s2 != "" else "0"
        return s2.zfill(width)
    return s


def canonicalize_tenant_id(value: Any) -> str:
    return canonicalize_digits(value, 10)


def canonicalize_module_id(value: Any) -> str:
    return canonicalize_digits(value, 6)


def canonicalize_reason_code(value: Any) -> str:
    # reason_code is GCCMMMMMMRRR (12 digits)
    return canonicalize_digits(value, 12)


def normalize_id(value: Any) -> str:
    """Normalize an identifier for matching/lookup.

    Examples:
      - "000001" -> "1"
      - 1 -> "1"
      - "0" / "000" -> "0"
      - " t0001 " -> "t0001" (non-digit preserved)
      - None -> "" (empty)

    IMPORTANT:
    - This function is intended for *matching keys*, not necessarily display formatting.
    """
    if value is None:
        return ""
    s = str(value).strip()
    if s == "":
        return ""
    if _DIGITS_RE.match(s):
        s2 = s.lstrip("0")
        return s2 if s2 != "" else "0"
    return s


def normalize_row_ids(row: MutableMapping[str, Any], id_fields: Iterable[str]) -> MutableMapping[str, Any]:
    """In-place normalize selected id fields in a dict-like row."""
    for f in id_fields:
        if f in row:
            row[f] = normalize_id(row[f])
    return row


def canonicalize_row_ids(
    row: MutableMapping[str, Any],
    id_fields: Iterable[str],
    *,
    widths: Mapping[str, int],
) -> MutableMapping[str, Any]:
    """In-place canonicalize selected id fields using per-field fixed widths.

    `widths` maps field name -> width. Any field not present in widths is left
    unchanged (after strip) to avoid corrupting alphanumeric IDs.
    """
    for f in id_fields:
        if f not in row:
            continue
        if f in widths:
            row[f] = canonicalize_digits(row[f], widths[f])
        else:
            # Safe fallback: trim whitespace only.
            v = row[f]
            row[f] = "" if v is None else str(v).strip()
    return row


@dataclass(frozen=True)
class DedupeResult:
    rows: list[dict[str, Any]]
    merged_count: int
    dropped_count: int


def _parse_sortable_ts(value: Any) -> Optional[str]:
    """Return a sortable timestamp string if present.

    We keep it simple and deterministic:
    - ISO-8601 strings sort lexicographically when in canonical form.
    - If formats differ, we still remain deterministic by string sort.

    Returns None if value is empty.
    """
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def dedupe_rows_by_normalized_id(
    rows: Iterable[Mapping[str, Any]],
    id_field: str,
    *,
    timestamp_fields: Iterable[str] = ("updated_at", "modified_at", "created_at", "received_at"),
    tie_breaker_fields: Iterable[str] = (),
    prefer: str = "latest",  # or "last"
) -> DedupeResult:
    """Deduplicate rows by normalized id_field with deterministic merge.

    Deterministic rules:
    - Key is normalize_id(row[id_field]).
    - If duplicates:
        * prefer='latest': keep row with greatest (most recent) timestamp across timestamp_fields.
          If no timestamps available, fall back to the later row in input (stable).
        * prefer='last': keep the later row in input (stable).
    - Non-kept rows are dropped (not combined arithmetically) to prevent accidental double counting.

    This is intentionally conservative for balance-like tables.
    """

    prefer = prefer.lower().strip()
    if prefer not in {"latest", "last"}:
        raise ValueError(f"prefer must be 'latest' or 'last', got: {prefer}")

    kept: dict[str, dict[str, Any]] = {}
    meta: dict[str, tuple[Optional[str], int, str]] = {}  # key -> (best_ts, best_idx, best_fingerprint)

    merged_count = 0
    dropped_count = 0

    for idx, r0 in enumerate(rows):
        r = dict(r0)
        raw = r.get(id_field, "")
        key = normalize_id(raw)
        r[id_field] = key

        # Build a deterministic fingerprint for tie-breaking (optional)
        fingerprint_parts = []
        for f in tie_breaker_fields:
            fingerprint_parts.append(str(r.get(f, "")).strip())
        fingerprint = "|".join(fingerprint_parts)

        if key not in kept:
            kept[key] = r
            best_ts = None
            if prefer == "latest":
                for tf in timestamp_fields:
                    ts = _parse_sortable_ts(r.get(tf))
                    if ts and (best_ts is None or ts > best_ts):
                        best_ts = ts
            meta[key] = (best_ts, idx, fingerprint)
            continue

        # Duplicate key
        prev_ts, prev_idx, prev_fp = meta[key]

        take_new = False

        if prefer == "last":
            take_new = True
        else:  # latest
            # compute new best timestamp
            new_ts = None
            for tf in timestamp_fields:
                ts = _parse_sortable_ts(r.get(tf))
                if ts and (new_ts is None or ts > new_ts):
                    new_ts = ts

            if prev_ts is None and new_ts is None:
                # No timestamps -> keep later row (stable)
                take_new = True
            elif prev_ts is None and new_ts is not None:
                take_new = True
            elif prev_ts is not None and new_ts is None:
                take_new = False
            else:
                # Both present
                if new_ts > prev_ts:
                    take_new = True
                elif new_ts < prev_ts:
                    take_new = False
                else:
                    # Tie on timestamp -> stable: keep later row; if tie_breaker_fields provided, use them
                    if fingerprint != prev_fp and tie_breaker_fields:
                        # lexicographically max fingerprint wins deterministically
                        take_new = fingerprint > prev_fp
                    else:
                        take_new = True

        if take_new:
            kept[key] = r
            best_ts = prev_ts
            if prefer == "latest":
                # update best_ts
                new_best_ts = None
                for tf in timestamp_fields:
                    ts = _parse_sortable_ts(r.get(tf))
                    if ts and (new_best_ts is None or ts > new_best_ts):
                        new_best_ts = ts
                best_ts = new_best_ts or prev_ts
            meta[key] = (best_ts, idx, fingerprint)
            dropped_count += 1
        else:
            dropped_count += 1

        merged_count += 1

    # Preserve deterministic output order by best_idx (the retained row position)
    ordered = sorted(meta.items(), key=lambda kv: kv[1][1])
    out_rows = [kept[k] for k, _ in ordered]

    return DedupeResult(rows=out_rows, merged_count=merged_count, dropped_count=dropped_count)
