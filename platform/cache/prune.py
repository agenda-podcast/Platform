from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

from ..github.actions_cache import ActionsCacheEntry, delete_cache, list_caches
from ..utils.csvio import read_csv, write_csv


CACHE_INDEX_HEADERS = [
    "row_type",
    "cache_key",
    "cache_key_prefix",
    "policy_name",
    "retention_days",
    "protected",
    "created_at",
    "last_accessed_at",
    "cache_id",
    "size_in_bytes",
    "notes",
]


@dataclass
class CacheManageResult:
    rules: int
    caches_seen: int
    caches_indexed: int
    deleted_caches: int
    would_delete_caches: int
    skipped_protected: int


def _parse_iso(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    s = s.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bool(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "y")


def _int(s: str, default: int) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default


@dataclass(frozen=True)
class _Rule:
    prefix: str
    name: str
    retention_days: int
    protected: bool


def _load_rules(rows: List[Dict[str, str]]) -> List[_Rule]:
    rules: List[_Rule] = []
    for r in rows:
        if str(r.get("row_type", "")).strip().upper() != "RULE":
            continue
        prefix = str(r.get("cache_key_prefix", "")).strip()
        name = str(r.get("policy_name", "")).strip() or "default"
        retention = _int(r.get("retention_days", ""), 180)
        protected = _bool(r.get("protected", "false"))
        rules.append(_Rule(prefix=prefix, name=name, retention_days=retention, protected=protected))
    # Ensure at least a fallback rule exists.
    if not any(rt.prefix == "" for rt in rules):
        rules.append(_Rule(prefix="", name="default", retention_days=180, protected=False))
    # Longest prefix wins
    rules.sort(key=lambda x: len(x.prefix), reverse=True)
    return rules


def _match_rule(key: str, rules: List[_Rule]) -> _Rule:
    for r in rules:
        if key.startswith(r.prefix):
            return r
    return rules[-1]


def _index_by_key(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        if str(r.get("row_type", "")).strip().upper() != "CACHE":
            continue
        k = str(r.get("cache_key", "")).strip()
        if k:
            out[k] = r
    return out


def run_cache_manage(
    repo_root: Path,
    cache_index_path: Path,
    *,
    apply: bool = False,
    delete_key: str = "",
    delete_prefix: str = "",
) -> CacheManageResult:
    """Centralized GitHub Actions cache management.

    - Uses a repo-managed cache_index.csv (policy + inventory) under platform/cache/.
    - Syncs inventory of existing repo caches into the file.
    - Optionally deletes caches either by retention policy or via surgical delete parameters.

    The cache index CSV supports two row types:
    - RULE rows: define policy by key prefix + retention days + default protection.
    - CACHE rows: inventory of actual caches and optional per-cache overrides.
    """

    cache_index_path = cache_index_path.resolve()
    if not cache_index_path.exists():
        raise FileNotFoundError(f"cache_index.csv not found at: {cache_index_path}")

    rows = read_csv(cache_index_path)
    rules = _load_rules(rows)
    existing_cache_rows = _index_by_key(rows)

    caches = list_caches()
    now = datetime.now(timezone.utc).replace(microsecond=0)

    deleted = 0
    would_delete = 0
    skipped_protected = 0

    # Build new CACHE inventory rows
    new_cache_rows: List[Dict[str, str]] = []

    # First pass: map caches into inventory with applied policy
    for c in caches:
        key = c.key.strip()
        rule = _match_rule(key, rules)

        prev = existing_cache_rows.get(key, {})

        # Allow per-cache overrides if present in existing file
        retention = _int(prev.get("retention_days", ""), rule.retention_days)
        protected = _bool(prev.get("protected", "")) if prev.get("protected") not in (None, "") else rule.protected

        created_at = c.created_at
        last_accessed_at = c.last_accessed_at

        new_cache_rows.append(
            {
                "row_type": "CACHE",
                "cache_key": key,
                "cache_key_prefix": rule.prefix,
                "policy_name": rule.name,
                "retention_days": str(retention),
                "protected": "true" if protected else "false",
                "created_at": created_at,
                "last_accessed_at": last_accessed_at,
                "cache_id": str(c.id),
                "size_in_bytes": str(c.size_in_bytes),
                "notes": str(prev.get("notes", "")).strip(),
            }
        )

    # Second pass: decide deletions
    def _should_delete(row: Dict[str, str]) -> bool:
        if _bool(row.get("protected", "false")):
            return False

        key = str(row.get("cache_key", "")).strip()
        if delete_key and key == delete_key:
            return True
        if delete_prefix and key.startswith(delete_prefix):
            return True

        # retention-based pruning (last accessed wins, fallback to created)
        retention_days = _int(row.get("retention_days", ""), 180)
        anchor = _parse_iso(row.get("last_accessed_at", ""))
        if anchor.year == 1970:
            anchor = _parse_iso(row.get("created_at", ""))
        expires = anchor + timedelta(days=retention_days)
        return expires <= now

    for row in new_cache_rows:
        if _bool(row.get("protected", "false")) and (
            (delete_key and row.get("cache_key") == delete_key)
            or (delete_prefix and str(row.get("cache_key", "")).startswith(delete_prefix))
        ):
            skipped_protected += 1
            continue

        if _should_delete(row):
            cid = int(str(row.get("cache_id", "0")) or "0")
            if cid <= 0:
                continue
            if apply:
                try:
                    delete_cache(cid)
                    deleted += 1
                except Exception:
                    # Intentionally swallow to keep workflow resilient
                    pass
            else:
                would_delete += 1

    # Persist updated index (RULE rows preserved, CACHE inventory rewritten)
    rule_rows: List[Dict[str, str]] = [r for r in rows if str(r.get("row_type", "")).strip().upper() == "RULE"]

    # Stable ordering: rules (as-is), then caches sorted by policy then key
    new_cache_rows.sort(key=lambda r: (str(r.get("policy_name", "")), str(r.get("cache_key", ""))))
    out_rows = rule_rows + new_cache_rows

    write_csv(cache_index_path, out_rows, CACHE_INDEX_HEADERS)

    return CacheManageResult(
        rules=len(rules),
        caches_seen=len(caches),
        caches_indexed=len(new_cache_rows),
        deleted_caches=deleted,
        would_delete_caches=would_delete,
        skipped_protected=skipped_protected,
    )


# Backwards-compatible alias (older name)
def run_cache_prune(
    billing_state_dir: Path,  # kept for compatibility; unused now
    dry_run: bool = True,
) -> CacheManageResult:
    repo_root = Path.cwd()
    idx = repo_root / "platform" / "cache" / "cache_index.csv"
    return run_cache_manage(repo_root, idx, apply=not dry_run)
