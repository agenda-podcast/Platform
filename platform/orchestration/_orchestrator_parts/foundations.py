"""Orchestrator implementation part (role-based split; kept <= 500 lines)."""

PART = r'''\
from __future__ import annotations

import json
import re
import os
import hashlib
import shutil
from datetime import date, datetime, timedelta, timezone
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from ..billing.state import BillingState
from ..common.id_codec import canon_module_id, canon_tenant_id, canon_work_order_id, id_key, dedupe_tenants_credits
from ..common.id_policy import generate_unique_id, validate_id
from ..github.releases import ensure_release, upload_release_assets, get_release_numeric_id, get_release_assets_numeric_ids
from ..orchestration.module_exec import execute_module_runner, derive_cache_key
from ..utils.csvio import read_csv
from ..utils.fs import ensure_dir
from ..utils.hashing import sha256_file
from ..utils.time import utcnow_iso
from ..infra.factory import InfraBundle
from ..infra.models import TransactionRecord, TransactionItemRecord, OutputRecord
from .idempotency import (
    key_workorder_spend,
    key_step_run,
    key_step_run_charge,
    key_deliverable_charge,
    key_delivery_evidence,
    key_refund,
)
from .status_reducer import StatusInputs, reduce_workorder_status

from ..secretstore.loader import load_secretstore, env_for_module


def _parse_iso_z(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)

def _parse_ttl_days_by_place_type(cfg: dict) -> dict[tuple[str, str], int]:
    """Parse platform_config.cache_ttl_policy.ttl_days_by_place_type into a mapping.

    Returns: {(place, type): days}
    Raises: ValueError on invalid or missing structures.
    """
    root = cfg.get('cache_ttl_policy') or {}
    entries = root.get('ttl_days_by_place_type')
    if not isinstance(entries, list):
        raise ValueError('Invalid platform_config.cache_ttl_policy.ttl_days_by_place_type: expected list')
    out: dict[tuple[str, str], int] = {}
    rx = re.compile(r'^(?P<place>[a-z0-9_]+):(?P<type>[a-z0-9_]+)=(?P<days>[0-9]+)$')
    for raw in entries:
        if not isinstance(raw, str):
            raise ValueError('Invalid platform_config.cache_ttl_policy.ttl_days_by_place_type entry: expected string')
        s = raw.strip()
        m = rx.match(s)
        if not m:
            raise ValueError(f"Invalid cache_ttl_policy.ttl_days_by_place_type entry {s!r} (expected 'place:type=days')")
        place = m.group('place')
        typ = m.group('type')
        days = int(m.group('days'))
        if days <= 0:
            raise ValueError(f"Invalid cache_ttl_policy.ttl_days_by_place_type entry {s!r} (days must be positive)")
        k = (place, typ)
        if k in out:
            raise ValueError(f"Duplicate cache_ttl_policy.ttl_days_by_place_type rules for place={place!r} type={typ!r}")
        out[k] = days
    return out


class PreflightSecretError(RuntimeError):
    def __init__(self, *, missing: list[dict[str, str]]):
        super().__init__("Missing required secrets for enabled steps")
        self.missing = missing


def _load_module_secret_requirements(repo_root: Path) -> dict[str, list[dict[str, str]]]:
    """Load secret requirements for modules from maintenance-state/module_requirements_index.csv.

    Row format:
      module_id,requirement_type,requirement_key,requirement_value,note

    Only requirement_type=secret rows are returned.
    """
    path = repo_root / "maintenance-state" / "module_requirements_index.csv"
    rows = read_csv(path) if path.exists() else []
    out: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        if str(r.get("requirement_type", "")).strip() != "secret":
            continue
        mid = canon_module_id(str(r.get("module_id", "")).strip())
        key = str(r.get("requirement_key", "")).strip()
        note = str(r.get("note", "")).strip()
        if not (mid and key):
            continue
        out.setdefault(mid, []).append({"key": key, "note": note})
    return out


def _is_secret_enforced(*, note: str) -> bool:
    """Determine if a secret requirement should be enforced.

    Rules:
      - If the requirement note contains "if unset" (case-insensitive), do not enforce (dev stub allowed).
      - Otherwise, enforce.
    """
    n = (note or "").lower()
    if "if unset" in n:
        return False
    return True


def _has_secret_value(*, store_env: dict[str, str], key: str, module_id: str) -> bool:
    """Check whether the secret is present via environment overrides or secretstore injection.

    A secret is considered present if:
      - os.environ has a non-empty value for the key (or module-prefixed key), OR
      - injected store env has a non-empty value for the key (or module-prefixed key).

    Empty string values are treated as missing.
    """
    candidates = [key, f"{module_id}_{key}"]
    for k in candidates:
        v = (os.environ.get(k) or "").strip()
        if v:
            return True
        sv = (store_env.get(k) or "").strip()
        if sv:
            return True
    return False

def _secret_presence_source(*, store_env: dict[str, str], key: str, module_id: str) -> str:
    """Return where the secret value is resolved from: env | secretstore | missing."""
    candidates = [key, f"{module_id}_{key}"]
    for k in candidates:
        v = (os.environ.get(k) or "").strip()
        if v:
            return "env"
        sv = (store_env.get(k) or "").strip()
        if sv:
            return "secretstore"
    return "missing"


def _preflight_assert_required_secrets(
    *,
    repo_root: Path,
    store: Any,
    plan: list[dict[str, Any]],
) -> None:
    """Preflight gate: ensure required secrets exist for all enabled steps.

    - Builds the set of enabled steps (provided as execution plan).
    - Resolves per-module secret requirements from maintenance-state/module_requirements_index.csv.
    - Asserts that every enforced secret exists in secretstore or env overrides.
    - Applies to all module kinds.

    Raises PreflightSecretError on missing enforced secrets.
    """
    reqs = _load_module_secret_requirements(repo_root)

    missing: list[dict[str, str]] = []
    for step in plan:
        sid = str(step.get("step_id") or "").strip()
        mid = canon_module_id(step.get("module_id") or "")
        if not mid:
            continue
        store_env = env_for_module(store, mid)

        enforced: list[str] = []
        present: list[dict[str, str]] = []
        missing_local: list[str] = []

        for rr in reqs.get(mid, []):
            key = rr["key"]
            note = rr.get("note", "")
            if not _is_secret_enforced(note=note):
                continue

            enforced.append(key)
            src_kind = _secret_presence_source(store_env=store_env, key=key, module_id=mid)
            if src_kind != "missing":
                present.append({"key": key, "source": src_kind})
                continue

            missing_local.append(key)
            missing.append({
                "step_id": sid,
                "module_id": mid,
                "secret_key": key,
                "note": note,
            })

        if enforced:
            present_compact = [f"{d['key']}@{d['source']}" for d in present]
            print(
                f"[preflight][secrets] step_id={sid} module_id={mid} "
                f"enforced={enforced} present={present_compact} missing={missing_local}"
            )

    if missing:
        raise PreflightSecretError(missing=missing)

def _cache_dirname(cache_key: str) -> str:
    """Stable, filesystem-safe directory name for a cache key."""
    h = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:16]
    return f"k-{h}"


def _dir_has_files(p: Path) -> bool:
    if not p.exists() or not p.is_dir():
        return False
    for _ in p.rglob("*"):
        return True
    return False


def _copy_tree(src: Path, dst: Path) -> None:
    """Copy directory tree contents from src to dst (dst recreated)."""
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    for fp in src.rglob("*"):
        if fp.is_dir():
            continue
        rel = fp.relative_to(src)
        outp = dst / rel
        outp.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fp, outp)


WORKORDERS_LOG_HEADERS = [
    "work_order_id","tenant_id","status","created_at","started_at","ended_at","note","metadata_json",
]

MODULE_RUNS_LOG_HEADERS = [
    "module_run_id","tenant_id","work_order_id","module_id","status","created_at","started_at","ended_at",
    "reason_code","report_path","output_ref","metadata_json",
]

TRANSACTIONS_HEADERS = [
    "transaction_id","tenant_id","work_order_id","type","amount_credits","created_at","reason_code","note","metadata_json",
]

TRANSACTION_ITEMS_HEADERS = [
    "transaction_item_id","transaction_id","tenant_id","module_id","work_order_id","step_id","deliverable_id","feature","type","amount_credits","created_at","note","metadata_json",
]
TENANTS_CREDITS_HEADERS = ["tenant_id","credits_available","updated_at","status"]

CACHE_INDEX_HEADERS = ["place","type","ref","created_at","expires_at"]

PROMOTION_REDEMPTIONS_HEADERS = ["redemption_id","tenant_id","promo_code","credits_granted","created_at","note","metadata_json"]

GITHUB_RELEASES_MAP_HEADERS = ["release_id","github_release_id","tag","tenant_id","work_order_id","created_at"]

GITHUB_ASSETS_MAP_HEADERS = ["asset_id","github_asset_id","release_id","asset_name","created_at"]


@dataclass(frozen=True)
class ReasonIndex:
    by_key: Dict[Tuple[str, str, str], str]  # (scope, module_id, reason_slug) -> reason_code
    refundable: Dict[str, bool]              # reason_code -> refundable


def _repo_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _is_binding(v: Any) -> bool:
    """Return True if a value is a workorder input binding object.

    Supported binding forms:
      - file selector binding: {from_step, from_file, selector, ...}
      - output binding (Option A): {from_step, output_id, ...}

    Backward-compatible keys:
      - step_id (alias for from_step)
      - from_output_id (alias for output_id)
    """
    if not isinstance(v, dict):
        return False
    has_step = bool(str(v.get("from_step") or v.get("step_id") or "").strip())
    if not has_step:
        return False
    has_from_file = bool(str(v.get("from_file") or "").strip())
    has_output_id = bool(str(v.get("output_id") or v.get("from_output_id") or "").strip())
    return has_from_file or has_output_id


def _collect_bind_deps(obj: Any) -> Set[str]:
    """Recursively collect from_step dependencies from an inputs spec."""
    deps: Set[str] = set()
    if _is_binding(obj):
        deps.add(str(obj.get("from_step") or obj.get("step_id") or "").strip())
        return deps
    if isinstance(obj, dict):
        for vv in obj.values():
            deps |= _collect_bind_deps(vv)
    elif isinstance(obj, list):
        for vv in obj:
            deps |= _collect_bind_deps(vv)
    return deps


def _json_path_get(obj: Any, path: str) -> Any:
    """Minimal JSONPath-like selector.

    Supported:
      - $.a.b
      - $.a[0].b
      - a.b (leading '$.' optional)
    """
    p = str(path or "").strip()
    if not p:
        return obj
    if p.startswith("$"):
        p = p[1:]
    if p.startswith("."):
        p = p[1:]
    cur: Any = obj
    if not p:
        return cur
    parts: List[str] = []
    buf = ""
    in_br = False
    for ch in p:
        if ch == "." and not in_br:
            if buf:
                parts.append(buf)
                buf = ""
            continue
        if ch == "[":
            in_br = True
        elif ch == "]":
            in_br = False
        buf += ch
    if buf:
        parts.append(buf)

    for part in parts:
        # handle foo[0][1]... and/or foo
        key = part
        idxs: List[int] = []
        if "[" in part:
            key = part.split("[", 1)[0]
            rest = part[len(key):]
            # parse indices
            tmp = ""
            for ch in rest:
                if ch.isdigit():
                    tmp += ch
                elif ch == "]":
                    if tmp:
                        idxs.append(int(tmp))
                        tmp = ""
            # ignore malformed
        if key:
            if not isinstance(cur, dict) or key not in cur:
                raise KeyError(f"json_path missing key: {key}")
            cur = cur[key]
        for i in idxs:
            if not isinstance(cur, list) or i >= len(cur):
                raise IndexError(f"json_path index out of range: {i}")
            cur = cur[i]
    return cur


def _discover_workorders(repo_root: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        return out
    for tdir in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not tdir.is_dir():
            continue
        tenant_yml = tdir / "tenant.yml"
        if not tenant_yml.exists():
            continue
        tcfg = _repo_yaml(tenant_yml)
        tenant_id = canon_tenant_id(tcfg.get("tenant_id", tdir.name))
        if not tenant_id:
            continue
        validate_id("tenant_id", tenant_id, "tenant_id")
        wdir = tdir / "workorders"
        if not wdir.exists():
            continue
        for wpath in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            w = _repo_yaml(wpath)
            wid = canon_work_order_id(w.get("work_order_id", wpath.stem))
            if not wid:
                continue
            validate_id("work_order_id", wid, "work_order_id")
'''

def get_part() -> str:
    return PART
