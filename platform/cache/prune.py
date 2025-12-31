from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from ..billing.state import BillingState
from ..github.actions_cache import delete_cache, list_caches
from ..utils.csvio import read_csv, write_csv
from ..utils.time import utcnow_iso


@dataclass
class CachePruneResult:
    updated_rows: int
    deleted_caches: int
    registered_orphans: int


def _parse_iso_z(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def run_cache_prune(billing_state_dir: Path, dry_run: bool = True) -> CachePruneResult:
    billing = BillingState(billing_state_dir)
    billing.validate_minimal()

    cache_index = read_csv(billing.path("cache_index.csv"))

    # 1) Fetch current repo caches from Actions.
    caches = list_caches()
    by_key = {c.key: c for c in caches}

    updated = 0
    deleted = 0
    registered = 0

    # 2) Fill in cache_id for known keys.
    for row in cache_index:
        k = str(row.get("cache_key", "")).strip()
        if not k:
            continue
        if row.get("cache_id"):
            continue
        entry = by_key.get(k)
        if entry:
            row["cache_id"] = str(entry.id)
            updated += 1

    # 3) Register orphans: caches that exist in GitHub but not in cache_index.
    known_keys = {str(r.get("cache_key", "")).strip() for r in cache_index if r.get("cache_key")}
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for c in caches:
        if c.key in known_keys:
            continue
        # Orphan => add with 1y hold as per design.
        hold_exp = now + timedelta(days=365)
        cache_index.append({
            "cache_key": c.key,
            "tenant_id": "",
            "module_id": "",
            "created_at": c.created_at,
            "expires_at": hold_exp.isoformat().replace("+00:00", "Z"),
            "cache_id": str(c.id),
        })
        registered += 1

    # 4) Delete expired caches with cache_id.
    for row in cache_index:
        cid = str(row.get("cache_id", "")).strip()
        if not cid:
            continue
        try:
            exp = _parse_iso_z(str(row.get("expires_at", "")))
        except Exception:
            continue
        if exp <= now:
            if not dry_run:
                try:
                    delete_cache(int(cid))
                    deleted += 1
                except Exception:
                    pass

    # Persist
    write_csv(billing.path("cache_index.csv"), cache_index, ["cache_key","tenant_id","module_id","created_at","expires_at","cache_id"])
    billing.write_state_manifest(["cache_index.csv"])

    return CachePruneResult(updated_rows=updated, deleted_caches=deleted, registered_orphans=registered)
