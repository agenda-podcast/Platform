from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from ..billing.state import BillingState
from ..github.actions_cache import delete_cache, list_caches
from ..utils.csvio import read_csv, write_csv


@dataclass
class CachePruneResult:
    rows_before: int
    rows_after: int
    deleted_caches: int


def _parse_iso_z(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def run_cache_prune(billing_state_dir: Path) -> CachePruneResult:
    """Prune expired GitHub Actions caches.

    Behavior (per policy):
      - Reads billing-state cache_index.csv
      - Removes expired caches from GitHub Actions cache storage
      - Removes corresponding rows from cache_index.csv
      - Writes nothing else

    Expected cache_index.csv headers:
      place,type,ref,created_at,expires_at

    Notes:
      - place,type are logical; this pruner currently acts on place=cache.
      - ref is treated as the Actions cache key.
    """

    billing = BillingState(billing_state_dir)
    billing.validate_minimal(required_files=["cache_index.csv"])

    rows = read_csv(billing.path("cache_index.csv"))
    rows_before = len(rows)

    now = datetime.now(timezone.utc).replace(microsecond=0)

    # Map current caches by key for deterministic lookup.
    caches = list_caches()
    by_key = {c.key: c for c in caches}

    kept: List[dict] = []
    deleted = 0

    for r in rows:
        place = str(r.get("place", "")).strip()
        ref = str(r.get("ref", "")).strip()
        exp_s = str(r.get("expires_at", "")).strip()

        # Only manage explicit cache entries.
        if place != "cache" or not ref:
            kept.append(r)
            continue

        try:
            exp = _parse_iso_z(exp_s)
        except Exception:
            # Invalid expiry is treated as non-expired to avoid destructive behavior.
            kept.append(r)
            continue

        if exp > now:
            kept.append(r)
            continue

        entry = by_key.get(ref)
        if entry is not None:
            try:
                delete_cache(int(entry.id))
                deleted += 1
            except Exception:
                # If deletion fails, keep the row so it can be retried.
                kept.append(r)
                continue
        # If the cache does not exist, drop the row anyway.

    # Persist: write only cache_index.csv (no manifest, no evidence).
    headers = ["place", "type", "ref", "created_at", "expires_at"]
    write_csv(billing.path("cache_index.csv"), kept, headers)

    return CachePruneResult(rows_before=rows_before, rows_after=len(kept), deleted_caches=deleted)
