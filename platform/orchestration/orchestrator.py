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
            out.append({"tenant_id": tenant_id, "work_order_id": wid, "workorder": w, "path": str(wpath)})
    return out


def _load_workorders_queue(repo_root: Path) -> Tuple[str, List[Dict[str, Any]]]:
    """Load workorders from maintenance-state/workorders_index.csv (canonical queue).

    Fallback: directory scan is allowed only when PLATFORM_DEV_SCAN_WORKORDERS=1 or index missing.
    Returns (queue_source, items).
    """
    # Optional override for verification runners.
    # When set, orchestrator uses this CSV as the canonical queue.
    # This keeps default behavior unchanged while enabling deterministic
    # selection for "verify a specific workorder" flows.
    override = str(os.environ.get('PLATFORM_WORKORDERS_INDEX_PATH', '') or '').strip()
    if override:
        o = Path(override)
        if o.is_absolute():
            idx_path = o
        else:
            idx_path = (repo_root / o).resolve()
    else:
        idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
    # Dev-only override: allow directory scan when explicitly enabled.
    # NOTE: this helper is used both in production code and unit tests; it must not
    # depend on locals from run_orchestrator().
    dev_scan = (str(os.environ.get('PLATFORM_DEV_SCAN_WORKORDERS', '') or '').strip() == '1')
    if (not idx_path.exists()) or dev_scan:
        return ('scan:tenants/*/workorders', _discover_workorders(repo_root))
    rows = read_csv(idx_path)
    out: List[Dict[str, Any]] = []
    for r in rows:
        enabled = str(r.get('enabled','')).strip().lower() == 'true'
        if not enabled:
            continue
        tenant_id = canon_tenant_id(r.get('tenant_id',''))
        work_order_id = canon_work_order_id(r.get('work_order_id',''))
        rel = str(r.get('path','')).strip()
        if not (tenant_id and work_order_id and rel):
            continue
        wpath = repo_root / rel
        if not wpath.exists():
            print(f'[orchestrator] WARNING: workorders_index references missing file: {rel}')
            continue
        w = _repo_yaml(wpath)
        out.append({'tenant_id': tenant_id, 'work_order_id': work_order_id, 'workorder': w, 'path': rel})
    src = str(idx_path)
    try:
        src = str(idx_path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        src = str(idx_path)
    return (src, out)


def _load_reason_index(repo_root: Path) -> ReasonIndex:
    ms = repo_root / "maintenance-state"
    catalog = read_csv(ms / "reason_catalog.csv")
    policy = read_csv(ms / "reason_policy.csv")

    by_key: Dict[Tuple[str, str, str], str] = {}
    for r in catalog:
        scope = str(r.get("scope", "")).strip().upper()
        module_id = str(r.get("module_id", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        code = str(r.get("reason_code", "")).strip()
        if not (scope and slug and code):
            continue
        if scope == "GLOBAL":
            module_id = ""
        by_key[(scope, module_id, slug)] = code

    refundable: Dict[str, bool] = {}
    for r in policy:
        code = str(r.get("reason_code", "")).strip()
        if not code:
            continue
        refundable[code] = str(r.get("refundable", "")).strip().lower() == "true"

    return ReasonIndex(by_key=by_key, refundable=refundable)


def _reason_code(idx: ReasonIndex, scope: str, module_id: str, reason_slug: str) -> str:
    scope_u = scope.upper()
    mid = module_id if scope_u == "MODULE" else ""
    code = idx.by_key.get((scope_u, mid, reason_slug))
    if code:
        return code
    # fallback: global unknown_error if present
    code = idx.by_key.get(("GLOBAL", "", "unknown_error"))
    return code or ""


def _parse_ymd(s: str) -> date:
    s = str(s or "").strip()
    if not s:
        return date(1970, 1, 1)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return date(1970, 1, 1)


def _load_module_prices(repo_root: Path) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Load per-deliverable pricing.

    Schema:
      module_id,deliverable_id,price_credits,effective_from,effective_to,active,notes

    deliverable_id="__run__" is reserved for the per-step execution charge.
    """
    rows = read_csv(repo_root / "platform" / "billing" / "module_prices.csv")
    out: Dict[str, Dict[str, Dict[str, str]]] = {}

    today = datetime.now(timezone.utc).date()

    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        did = str(r.get("deliverable_id", "") or "").strip()
        if not mid or not did:
            continue

        active = str(r.get("active", "") or "").strip().lower() == "true"
        if not active:
            continue

        eff_from = _parse_ymd(r.get("effective_from", ""))
        eff_to_raw = str(r.get("effective_to", "") or "").strip()
        eff_to = _parse_ymd(eff_to_raw) if eff_to_raw else date(2100, 1, 1)
        if not (eff_from <= today <= eff_to):
            continue

        # If multiple rows match, choose the most recent effective_from.
        by_del = out.setdefault(mid, {})
        prev = by_del.get(did)
        if prev is not None:
            prev_from = _parse_ymd(prev.get("effective_from", ""))
            if prev_from >= eff_from:
                continue
        by_del[did] = r

    return out


def _price(prices: Dict[str, Dict[str, Dict[str, str]]], module_id: str, deliverable_id: str) -> int:
    mid = canon_module_id(module_id)
    did = str(deliverable_id or "").strip()
    if not mid or not did:
        return 0
    row = (prices.get(mid) or {}).get(did)
    if not row:
        return 0
    try:
        return int(str(row.get("price_credits", "0")).strip() or "0")
    except Exception:
        return 0


def _price_breakdown_for_step(
    prices: Dict[str, Dict[str, Dict[str, str]]],
    module_id: str,
    requested_deliverables: List[str],
) -> Dict[str, int]:
    """Return per-step pricing breakdown keyed by deliverable_id (including __run__)."""
    out: Dict[str, int] = {"__run__": _price(prices, module_id, "__run__")}
    for did in requested_deliverables or []:
        ds = str(did or "").strip()
        if not ds:
            continue
        out[ds] = _price(prices, module_id, ds)
    return out


def _sum_prices(breakdown: Dict[str, int]) -> int:
    return int(sum(int(v) for v in (breakdown or {}).values()))
def _load_module_display_names(registry: Any) -> Dict[str, str]:
    """Load optional human-readable module names using registry.get_contract (key: module_id)."""
    out: Dict[str, str] = {}
    try:
        mids = list(registry.list_modules())
    except Exception:
        mids = []
    for mid in mids:
        try:
            c = registry.get_contract(mid)
        except Exception:
            continue
        cmid = canon_module_id(c.get("module_id") or mid)
        if not cmid:
            continue
        name = str(c.get("name") or "").strip()
        if name:
            out[cmid] = name
    return out


def _load_module_ports(registry: Any, module_id: str) -> Dict[str, Any]:
    """Load module port definitions using registry.get_contract(module_id).

    This preserves the older return shape expected by _ports_index while avoiding direct filesystem reads.
    """
    mid = canon_module_id(module_id)
    if not mid:
        raise ValueError(f"Invalid module_id for ports: {module_id!r}")
    contract = registry.get_contract(mid)
    inputs = contract.get("inputs") or {}
    if not isinstance(inputs, dict):
        inputs = {}

    in_port = [v for v in inputs.values() if isinstance(v, dict) and not bool(v.get("is_limited"))]
    in_limited = [v for v in inputs.values() if isinstance(v, dict) and bool(v.get("is_limited"))]

    outputs_map = contract.get("outputs") or {}
    out_port = []
    out_limited = []
    if isinstance(outputs_map, dict):
        for o in outputs_map.values():
            if not isinstance(o, dict):
                continue
            # Registry does not encode limited vs non-limited outputs; treat all as tenant outputs.
            out_port.append(o)

    return {"inputs_port": in_port, "inputs_limited_port": in_limited, "outputs_port": out_port, "outputs_limited_port": out_limited}


def _ports_index(ports: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Set[str]]:
    """Return (tenant_inputs, platform_inputs, tenant_output_paths)."""
    tenant_inputs: Dict[str, Dict[str, Any]] = {}
    platform_inputs: Dict[str, Dict[str, Any]] = {}
    tenant_output_paths: Set[str] = set()

    for p in ports.get("inputs_port") or []:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        if pid:
            tenant_inputs[pid] = p

    for p in ports.get("inputs_limited_port") or []:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        if pid:
            platform_inputs[pid] = p

    for p in ports.get("outputs_port") or []:
        if not isinstance(p, dict):
            continue
        path = str(p.get("path") or "").lstrip("/").strip()
        if path:
            tenant_output_paths.add(path)

    return tenant_inputs, platform_inputs, tenant_output_paths


# ------------------------------------------------------------------
# Deliverables contract (module.yml: deliverables.port)
# ------------------------------------------------------------------

def _load_module_deliverables(registry: Any, module_id: str) -> Dict[str, Dict[str, Any]]:
    """Load deliverables contract using registry.get_contract (no direct filesystem reads)."""
    mid = canon_module_id(module_id)
    if not mid:
        raise ValueError(f"Invalid module_id for deliverables: {module_id!r}")
    contract = registry.get_contract(mid)
    dmap = contract.get("deliverables") or {}
    if not isinstance(dmap, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for did, d in dmap.items():
        if not isinstance(d, dict):
            continue
        out[str(did)] = {"limited_inputs": d.get("limited_inputs") or {}, "output_paths": d.get("output_paths") or []}
    return out


def _normalize_requested_deliverables(
    repo_root: Path,
    registry: Any,
    module_id: str,
    cfg: Dict[str, Any],
    deliverables_cache: Dict[str, Dict[str, Dict[str, Any]]],
) -> Tuple[List[str], str]:
    """Return (requested_deliverables, source).

    Backward-compat mapping:
      - if cfg.deliverables missing and legacy purchase_release_artifacts: true
        map to ['tenant_outputs'] if present; else all deliverables declared by module

    Registry
      Uses registry.get_contract(module_id) to resolve available deliverables.
    """

    _ = repo_root

    if "deliverables" in cfg and cfg.get("deliverables") is not None:
        raw = cfg.get("deliverables")
        if not isinstance(raw, list):
            raise ValueError(f"step.deliverables must be a list for module {module_id}")
        out: List[str] = []
        seen: Set[str] = set()
        for x in raw:
            s = str(x or "").strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out, "explicit"

    if bool(cfg.get("purchase_release_artifacts", False)):
        mid = canon_module_id(module_id)
        if mid not in deliverables_cache:
            try:
                contract = registry.get_contract(mid)
            except Exception:
                contract = {}
            dmap = contract.get("deliverables") or {}
            if not isinstance(dmap, dict):
                dmap = {}
            # Normalize to the shape used by downstream helpers
            norm: Dict[str, Dict[str, Any]] = {}
            for did, d in dmap.items():
                if not isinstance(d, dict):
                    continue
                norm[str(did)] = {
                    "limited_inputs": dict(d.get("limited_inputs") or {}),
                    "output_paths": list(d.get("output_paths") or []),
                }
            deliverables_cache[mid] = norm
        contract2 = deliverables_cache[mid]
        if "tenant_outputs" in contract2:
            return ["tenant_outputs"], "legacy:tenant_outputs"
        if contract2:
            return sorted(contract2.keys()), "legacy:all"
        return [], "legacy:none"

    return [], "none"


def _union_limited_inputs(contract: Dict[str, Dict[str, Any]], requested: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for did in requested:
        d = contract.get(did) or {}
        lim = d.get("limited_inputs") or {}
        if isinstance(lim, dict):
            for k, v in lim.items():
                out[str(k)] = v
    return out


def _deliverable_output_paths(contract: Dict[str, Dict[str, Any]], requested: List[str]) -> List[str]:
    paths: List[str] = []
    seen: Set[str] = set()
    for did in requested:
        d = contract.get(did) or {}
        ops = d.get("output_paths") or []
        if not isinstance(ops, list):
            continue
        for pth in ops:
            ps = str(pth or "").lstrip("/").strip()
            if not ps or ps in seen:
                continue
            seen.add(ps)
            paths.append(ps)
    return paths


def _effective_inputs_hash(inputs: Any) -> str:
    try:
        payload = json.dumps(inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = repr(inputs)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _rel_path_allowed(rel: str, allowed_paths: List[str]) -> bool:
    if not allowed_paths:
        return True
    rel = str(rel).lstrip("/")
    for p in allowed_paths:
        ps = str(p).lstrip("/")
        if not ps:
            continue
        if rel == ps or rel.startswith(ps + "/"):
            return True
    return False


def _toposort_nodes(nodes: List[str], edges: Dict[str, Set[str]]) -> List[str]:
    """Topologically sort nodes based on dependency edges.

    Args:
        nodes: list of node ids to sort (order is preserved where possible)
        edges: mapping node -> set(dependency nodes)

    Returns:
        ordered list where dependencies appear before dependents
    """
    ordered: List[str] = []
    temp: Set[str] = set()
    perm: Set[str] = set()

    def visit(n: str) -> None:
        if n in perm:
            return
        if n in temp:
            raise ValueError(f"Cycle in dependencies at {n}")
        temp.add(n)
        for d in sorted(edges.get(n, set())):
            visit(d)
        temp.remove(n)
        perm.add(n)
        ordered.append(n)

    for n in nodes:
        visit(n)
    return ordered


def _load_binding_value(step_outputs_dir: Path, binding: Dict[str, Any]) -> Any:
    """Load and transform a value from an upstream step output file."""
    rel_file = str(binding.get("from_file") or binding.get("file") or "").strip()
    if not rel_file:
        raise FileNotFoundError("binding.from_file is required")
    selector = str(binding.get("selector") or "").strip().lower() or "text"
    take = binding.get("take")
    take_n: Optional[int] = None
    try:
        if take is not None:
            take_n = int(take)
    except Exception:
        take_n = None

    fp = step_outputs_dir / rel_file
    if not fp.exists() or not fp.is_file():
        raise FileNotFoundError(str(fp))

    if selector == "text":
        return fp.read_text(encoding="utf-8", errors="replace")

    if selector == "lines":
        lines = [ln.strip() for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines()]
        lines = [ln for ln in lines if ln]
        if take_n is not None:
            lines = lines[: max(0, take_n)]
        return lines

    if selector == "json":
        data = json.loads(fp.read_text(encoding="utf-8", errors="replace") or "null")
        jp = str(binding.get("json_path") or "").strip()
        return _json_path_get(data, jp) if jp else data

    if selector == "jsonl_first":
        first = None
        for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines():
            if ln.strip():
                first = ln
                break
        if first is None:
            raise ValueError("jsonl_first: file is empty")
        data = json.loads(first)
        jp = str(binding.get("json_path") or "").strip()
        return _json_path_get(data, jp) if jp else data

    if selector == "jsonl":
        out: List[Any] = []
        for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines():
            s = ln.strip()
            if not s:
                continue
            out.append(json.loads(s))
            if take_n is not None and len(out) >= take_n:
                break
        return out

    raise ValueError(f"Unsupported binding selector: {selector}")


def _extract_step_edges(steps: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """Infer step dependencies from input bindings (from_step)."""
    edges: Dict[str, Set[str]] = {}
    known = {str(s.get("step_id") or "").strip() for s in steps if str(s.get("step_id") or "").strip()}
    for s in steps:
        sid = str(s.get("step_id") or "").strip()
        if not sid:
            continue
        deps = {d for d in _collect_bind_deps(s.get("inputs") or {}) if d in known}
        # Optional explicit dependencies
        explicit = s.get("depends_on") or s.get("needs")
        if isinstance(explicit, list):
            for x in explicit:
                xs = str(x).strip()
                if xs and xs in known:
                    deps.add(xs)
        edges[sid] = deps
    return edges


def _resolve_inputs(
    inputs_spec: Any,
    step_outputs: Dict[str, Path],
    allowed_outputs: Dict[str, Set[str]],
    run_state: Any,
    tenant_id: str,
    work_order_id: str,
) -> Any:
    """Resolve bindings within an inputs spec.

    Supported binding forms:
      - file selector binding: {from_step, from_file, selector, ...}
      - output binding (Option A): {from_step, output_id, ...} returning a resolved OutputRecord dict

    Rules:
      - bindings may appear at any depth (inside dicts/lists)
      - for file bindings, from_file must be exposed by upstream step (tenant-visible outputs)
      - for output bindings, output_id must exist in run_state and its output path must be exposed
    """
    if _is_binding(inputs_spec):
        src = inputs_spec
        if isinstance(inputs_spec.get("from"), dict):
            src = inputs_spec.get("from") or {}

        from_step = str(src.get("from_step") or src.get("step_id") or "").strip()
        if not from_step:
            raise ValueError("binding.from_step is required")
        if from_step not in step_outputs:
            raise FileNotFoundError(f"Upstream step outputs not found: {from_step}")

        output_id = str(src.get("output_id") or src.get("from_output_id") or "").strip()
        if output_id:
            rec = run_state.get_output(tenant_id, work_order_id, from_step, output_id)
            allowed = allowed_outputs.get(from_step) or set()
            if allowed and rec.path and rec.path not in allowed:
                raise PermissionError(
                    f"binding.output_id '{output_id}' is not exposed by upstream step '{from_step}' (allowed: {sorted(allowed)})"
                )
            out = asdict(rec)
            if "as_path" in src:
                out["as_path"] = src.get("as_path")
            elif "as" in src:
                out["as_path"] = src.get("as")
            return out

        from_file = str(src.get("from_file") or "").lstrip("/").strip()
        if not from_file:
            raise ValueError("binding.from_file or binding.output_id is required")

        allowed = allowed_outputs.get(from_step) or set()
        if allowed and from_file not in allowed:
            raise PermissionError(
                f"binding.from_file '{from_file}' is not exposed by upstream step '{from_step}' (allowed: {sorted(allowed)})"
            )
        return _load_binding_value(step_outputs[from_step], src)

    if isinstance(inputs_spec, dict):
        return {k: _resolve_inputs(v, step_outputs, allowed_outputs, run_state, tenant_id, work_order_id) for k, v in inputs_spec.items()}
    if isinstance(inputs_spec, list):
        return [_resolve_inputs(v, step_outputs, allowed_outputs, run_state, tenant_id, work_order_id) for v in inputs_spec]
    return inputs_spec
def _build_execution_plan(workorder: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize a workorder into an ordered execution plan.

    Returns:
      - plan: list of dicts with keys: step_id, module_id, cfg
    """
    steps = workorder.get("steps")
    if not (isinstance(steps, list) and steps):
        raise ValueError("Workorder must define non-empty 'steps' list (legacy modules-only workorders are not supported)")

    plan_steps: List[Dict[str, Any]] = []
    for s in steps:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("step_id") or "").strip()
        mid = canon_module_id(s.get("module_id") or "")
        if not sid or not mid:
            continue
        validate_id("step_id", sid, "workorder.step.step_id")
        plan_steps.append({"step_id": sid, "module_id": mid, "cfg": s})

    edges = _extract_step_edges([p["cfg"] for p in plan_steps])
    # edges keys refer to step_id; ensure node list order is stable as in YAML
    nodes = [p["step_id"] for p in plan_steps]
    ordered_sids = _toposort_nodes(nodes, edges)
    by_id = {p["step_id"]: p for p in plan_steps}
    return [by_id[sid] for sid in ordered_sids if sid in by_id]


def _load_tenant_relationships(repo_root: Path) -> Set[Tuple[str, str]]:
    rows = read_csv(repo_root / "maintenance-state" / "tenant_relationships.csv")
    out=set()
    for r in rows:
        s = canon_tenant_id(r.get("source_tenant_id",""))
        t = canon_tenant_id(r.get("target_tenant_id",""))
        if s and t:
            out.add((s,t))
    return out


def _load_module_artifacts_policy(repo_root: Path) -> Dict[str, bool]:
    rows = read_csv(repo_root / "maintenance-state" / "module_artifacts_policy.csv")
    out: Dict[str, bool] = {}
    for r in rows:
        mid = canon_module_id(r.get("module_id",""))
        if not mid:
            continue
        out[mid] = str(r.get("platform_artifacts_enabled","")).strip().lower() == "true"
    return out


def _new_id(id_type: str, used: Set[str]) -> str:
    return generate_unique_id(id_type, used)


@dataclass(frozen=True)
class OrchestratorContext:
    tenant_id: str
    work_order_id: str
    run_id: str
    runtime_profile_name: str


def run_orchestrator(repo_root: Path, billing_state_dir: Path, runtime_dir: Path, enable_github_releases: bool = False, infra: InfraBundle | None = None) -> None:
    if infra is None:
        from ..infra.config import load_runtime_profile
        from ..infra.factory import build_infra

        profile = load_runtime_profile(repo_root, cli_path="")
        infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    registry = infra.registry
    run_state = infra.run_state_store
    ledger = infra.ledger_writer
    runtime_profile_name = str(infra.profile.profile_name or "").strip() or "default"

    # Environment reads are centralized at the orchestration entrypoint.
    dev_scan_workorders = (str(os.environ.get('PLATFORM_DEV_SCAN_WORKORDERS', '') or '').strip() == '1')
    secretstore_passphrase_present = bool(str(os.environ.get('SECRETSTORE_PASSPHRASE', '') or '').strip())
    github_token_present = bool(os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN'))

    # Preflight validator (no execution). Enabled workorders must pass before any billing or execution.
    from platform.consistency.validator import ConsistencyValidationError, load_rules_table, validate_workorder_preflight

    module_rules_by_id = load_rules_table(repo_root)
    run_since = utcnow_iso()
    from platform.config.load_platform_config import load_platform_config
    platform_cfg = load_platform_config(repo_root)
    cache_ttl_days_by_place_type = _parse_ttl_days_by_place_type(platform_cfg)
    cache_ttl_days = cache_ttl_days_by_place_type.get(('cache','module_run'))
    if cache_ttl_days is None:
        raise ValueError("Missing cache TTL rule for 'cache:module_run' in platform/config/platform_config.yml")

    reason_idx = _load_reason_index(repo_root)
    tenant_rel = _load_tenant_relationships(repo_root)
    prices = _load_module_prices(repo_root)
    artifacts_policy = _load_module_artifacts_policy(repo_root)
    module_names = _load_module_display_names(registry)

    billing = BillingState(billing_state_dir)
    billing_state_dir.mkdir(parents=True, exist_ok=True)

    # Orchestrator needs full state including mapping tables
    billing.validate_minimal(
        required_files=[
            "tenants_credits.csv",
            "transactions.csv",
            "transaction_items.csv",
            "promotion_redemptions.csv",
            "cache_index.csv",
                                    "github_releases_map.csv",
            "github_assets_map.csv",
        ]
    )

    tenants_credits = dedupe_tenants_credits(billing.load_table("tenants_credits.csv"))
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")
    cache_index = billing.load_table("cache_index.csv")
    promo_redemptions = billing.load_table("promotion_redemptions.csv")
    rel_map = billing.load_table("github_releases_map.csv")
    asset_map = billing.load_table("github_assets_map.csv")

    used_tx: Set[str] = {id_key(r.get("transaction_id")) for r in transactions if id_key(r.get("transaction_id"))}
    used_ti: Set[str] = {id_key(r.get("transaction_item_id")) for r in transaction_items if id_key(r.get("transaction_item_id"))}
    used_mr: Set[str] = set()
    used_rel: Set[str] = {id_key(r.get("release_id")) for r in rel_map if id_key(r.get("release_id"))}
    used_asset: Set[str] = {id_key(r.get("asset_id")) for r in asset_map if id_key(r.get("asset_id"))}

    runtime_dir.mkdir(parents=True, exist_ok=True)
    ensure_dir(runtime_dir)

    # Local module output cache (persisted across workflow runs via actions/cache).
    cache_root = runtime_dir / "cache_outputs"
    ensure_dir(cache_root)

    queue_source, workorders = _load_workorders_queue(repo_root)

    # Module deliverables contracts cached per run
    deliverables_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # Load encrypted secretstore once per run (if configured).
    store = load_secretstore(repo_root)
    if store.version <= 0:
        if secretstore_passphrase_present:
            print('[secretstore] WARNING: passphrase provided but store version is 0 (decrypt/parse may have failed).')
        else:
            print('[secretstore] INFO: SECRETSTORE_PASSPHRASE not provided; proceeding without injected secrets.')
    else:
        mods = store.raw.get('modules') or {}
        if isinstance(mods, dict):
            print(f"[secretstore] loaded version={store.version} modules={sorted(mods.keys())}")


    # Run-scoped summary (useful in CI logs)
    print("\nORCHESTRATOR RUN-SCOPED SUMMARY")
    print(f"billing_state_dir: {billing_state_dir}")
    print(f"runtime_dir:       {runtime_dir}")
    print(f"tenants_dir:       {repo_root / 'tenants'}")
    print(f"since:             {run_since}")
    print("")
    print(f"queue_source:       {queue_source}")
    print(f"Queued workorders:  {len(workorders)}")
    for it in workorders:
        print(f" - {it.get('path')}")
    print("")


    # Auto-enable GitHub Releases when artifacts were purchased.
    # This keeps offline/local runs stable while ensuring "download artifacts" works in GitHub Actions.
    if not enable_github_releases:
        token_present = github_token_present
        if token_present:
            wants_releases = False
            for it in workorders:
                w = dict(it.get("workorder") or {})
                if not bool(w.get("enabled", True)):
                    continue
                _plan = _build_execution_plan(w)
                for step in _plan:
                    mid = canon_module_id(step.get("module_id", ""))
                    cfg = dict(step.get("cfg") or {})
                    if not mid:
                        continue
                    req, _src = _normalize_requested_deliverables(repo_root, registry, mid, cfg, deliverables_cache)
                    if bool(req) and artifacts_policy.get(mid, True):
                        wants_releases = True
                        break
                if wants_releases:
                    break
            if wants_releases:
                enable_github_releases = True

    for item in workorders:
        w = dict(item["workorder"])
        if not bool(w.get("enabled", True)):
            continue

        tenant_id = canon_tenant_id(item["tenant_id"])
        work_order_id = canon_work_order_id(item["work_order_id"])
        # Preflight validation hook: enabled workorders must be valid; drafts (disabled) are allowed to exist.
        workorder_path = repo_root / str(item.get("path") or "")
        try:
            validate_workorder_preflight(repo_root, workorder_path, module_rules_by_id)
        except ConsistencyValidationError as e:
            raise RuntimeError(f"Workorder preflight failed for {workorder_path}: {e}") from e

        created_at = str((w.get("metadata") or {}).get("created_at") or utcnow_iso())
        started_at = utcnow_iso()

        # pricing + execution plan
        plan_type = "steps"
        plan = _build_execution_plan(w)
        print(f"[orchestrator] work_order_id={work_order_id} tenant_id={tenant_id} plan_type={plan_type} steps={[p.get('step_id') for p in plan]}")

        est_total = 0
        per_step_requested_deliverables: Dict[str, List[str]] = {}
        per_step_deliverables_source: Dict[str, str] = {}
        per_step_price_breakdown: Dict[str, Dict[str, int]] = {}
        for step in plan:
            sid = str(step.get("step_id") or '').strip()
            mid = canon_module_id(step.get("module_id", ""))
            cfg = dict(step.get("cfg") or {})
            req_deliverables, del_src = _normalize_requested_deliverables(repo_root, registry, mid, cfg, deliverables_cache)
            breakdown = _price_breakdown_for_step(prices, mid, req_deliverables)
            if sid:
                per_step_requested_deliverables[sid] = req_deliverables
                per_step_deliverables_source[sid] = del_src
                per_step_price_breakdown[sid] = breakdown
            est_total += _sum_prices(breakdown)

        # Record run context in adapterized run-state store (append-only, latest-wins semantics).
        artifacts_requested = False
        for _sid, _dids in per_step_requested_deliverables.items():
            if _dids:
                artifacts_requested = True
                break

        ctx = OrchestratorContext(
            tenant_id=tenant_id,
            work_order_id=work_order_id,
            run_id=work_order_id,
            runtime_profile_name=runtime_profile_name,
        )
        try:
            run_state.create_run(
                tenant_id=ctx.tenant_id,
                work_order_id=ctx.work_order_id,
                metadata={
                    "workorder_path": str(item.get("path") or ""),
                    "runtime_profile_name": ctx.runtime_profile_name,
                    "artifacts_requested": artifacts_requested,
                    "requested_deliverables_by_step": per_step_requested_deliverables,
                    "deliverables_source_by_step": per_step_deliverables_source,
                    "any_delivery_missing": False,
                },
            )
            run_state.set_run_status(
                tenant_id=ctx.tenant_id,
                work_order_id=ctx.work_order_id,
                status="RUNNING",
                metadata={"runtime_profile_name": ctx.runtime_profile_name},
            )
        except Exception:
            # Orchestrator continues even if run-state logging fails (dev mode ergonomics).
            pass

        # Preflight secret requirements gate (all module kinds).
        try:
            _preflight_assert_required_secrets(repo_root=repo_root, store=store, plan=plan)
        except PreflightSecretError as e:
            rc = _reason_code(reason_idx, "GLOBAL", "", "secrets_missing")
            human_note = "Preflight failed: missing required secrets for one or more enabled steps"
            ended = utcnow_iso()
            # Emit a deterministic, grep-friendly console log so GitHub Actions users can
            # immediately see what is missing without opening CSV artifacts.
            try:
                missing_compact = [
                    f"{m.get('step_id')}:{m.get('module_id')}:{m.get('secret_key')}" for m in (e.missing or [])
                ]
            except Exception:
                missing_compact = []
            print(f"[preflight][FAILED] work_order_id={work_order_id} reason_code={rc} missing={missing_compact}")
            meta = {
                "workorder_path": str(item.get("path") or ""),
                "reason_code": rc,
                "missing_secrets": e.missing,
            }
            workorders_log.append({
                "work_order_id": work_order_id,
                "tenant_id": tenant_id,
                "status": "FAILED",
                "created_at": created_at,
                "started_at": started_at,
                "ended_at": ended,
                "note": human_note,
                "metadata_json": json.dumps(meta, separators=(",", ":")),
            })
            try:
                run_state.set_run_status(
                    tenant_id=ctx.tenant_id,
                    work_order_id=ctx.work_order_id,
                    status="FAILED",
                    metadata={
                        "reason_code": rc,
                        "missing_secrets": e.missing,
                        "note": human_note,
                        "ended_at": ended,
                    },
                )
            except Exception:
                pass
            continue


        # current balance
        trow = None
        for r in tenants_credits:
            if canon_tenant_id(r.get("tenant_id","")) == tenant_id:
                trow = r
                break
        if not trow:
            trow = {"tenant_id": tenant_id, "credits_available": "0", "updated_at": utcnow_iso(), "status": "ACTIVE"}
            tenants_credits.append(trow)
        available = int(str(trow.get("credits_available","0")).strip() or "0")

        if available < est_total:
            rc = _reason_code(reason_idx, "GLOBAL", "", "not_enough_credits")
            human_note = f"Insufficient credits: available={available}, required={est_total}"

            workorders_log.append({
                    "work_order_id": work_order_id,
                    "tenant_id": tenant_id,
                    "status": "FAILED",
                    "created_at": created_at,
                    "started_at": started_at,
                    "ended_at": utcnow_iso(),
                    "note": human_note,
                    "metadata_json": json.dumps({"workorder_path": item["path"], "reason_code": rc, "available": available, "required": est_total}, separators=(",", ":")),
                })
            continue

        # spend transaction (debit) with idempotency
        spend_idem = key_workorder_spend(tenant_id=tenant_id, work_order_id=work_order_id, workorder_path=str(item["path"]), plan_type=plan_type)
        spend_tx = ""
        for tx in transactions:
            if str(tx.get("tenant_id")) != tenant_id or str(tx.get("work_order_id")) != work_order_id:
                continue
            if str(tx.get("type")) != "SPEND":
                continue
            try:
                meta = json.loads(str(tx.get("metadata_json") or "{}")) if str(tx.get("metadata_json") or "").strip() else {}
            except Exception:
                meta = {}
            if str(meta.get("idempotency_key")) == spend_idem:
                spend_tx = str(tx.get("transaction_id"))
                break
        if not spend_tx:
            spend_tx = _new_id("transaction_id", used_tx)

        def _label(mid: str, sid: str, sname: str = "") -> str:
            base = f"{module_names.get(mid, mid)} ({mid})" if module_names.get(mid) else mid
            human = (sname or "").strip()
            # step_id is the stable identifier used for wiring/IO; step_name is only for UX/logs
            if sid and human:
                return f"{base} {human} [{sid}]"
            if sid and sid != mid:
                return f"{base} [{sid}]"
            return base

        plan_human = ", ".join([
            _label(
                str(p.get("module_id")),
                str(p.get("step_id")),
                str((p.get("cfg") or {}).get("step_name") or (p.get("cfg") or {}).get("name") or ""),
            )
            for p in plan
        ])

        if not any(str(tx.get("transaction_id")) == spend_tx for tx in transactions):
            tx_meta = {"workorder_path": item["path"], "plan_type": plan_type, "steps": [p.get("step_id") for p in plan], "idempotency_key": spend_idem}
            transactions.append({
                "transaction_id": spend_tx,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "SPEND",
                "amount_credits": str(-est_total),
                "created_at": utcnow_iso(),
                "reason_code": "",
                "note": f"Work order spend: {plan_human}",
                "metadata_json": json.dumps(tx_meta, separators=(",", ":")),
            })
            try:
                ledger.post_transaction(TransactionRecord(
                    transaction_id=spend_tx,
                    tenant_id=tenant_id,
                    work_order_id=work_order_id,
                    type="SPEND",
                    amount_credits=-int(est_total),
                    created_at=utcnow_iso(),
                    note=f"Work order spend: {plan_human}",
                    metadata_json=json.dumps(tx_meta, separators=(",", ":")),
                ))
            except Exception:
                pass

        # transaction items per step (audit + refunds)
        per_step_cost: Dict[str, int] = {}
        per_step_prices: Dict[str, Dict[str, int]] = {}
        for step in plan:
            sid = str(step.get("step_id") or "").strip()
            mid = canon_module_id(step.get("module_id") or "")
            cfg = dict(step.get("cfg") or {})
            sname = str(cfg.get("step_name") or cfg.get("name") or "").strip()
            req_deliverables = per_step_requested_deliverables.get(sid, []) or []
            del_src = per_step_deliverables_source.get(sid, "none")

            breakdown = per_step_price_breakdown.get(sid) or _price_breakdown_for_step(prices, mid, req_deliverables)
            per_step_prices[sid] = breakdown
            cost = _sum_prices(breakdown)
            per_step_cost[sid] = cost

            m_label = _label(mid, sid, sname)

            def _append_tx_item(item_row: Dict[str, Any]) -> None:
                try:
                    meta = json.loads(str(item_row.get("metadata_json") or "{}")) if str(item_row.get("metadata_json") or "").strip() else {}
                except Exception:
                    meta = {}
                idem = str(meta.get("idempotency_key") or "")
                if idem:
                    for existing in transaction_items:
                        try:
                            em = json.loads(str(existing.get("metadata_json") or "{}")) if str(existing.get("metadata_json") or "").strip() else {}
                        except Exception:
                            em = {}
                        if str(em.get("idempotency_key")) == idem:
                            return
                transaction_items.append(item_row)
                try:
                    ledger.post_transaction_item(TransactionItemRecord(
                        transaction_item_id=str(item_row.get("transaction_item_id")),
                        transaction_id=str(item_row.get("transaction_id")),
                        tenant_id=str(item_row.get("tenant_id")),
                        module_id=str(item_row.get("module_id")),
                        work_order_id=str(item_row.get("work_order_id")),
                        step_id=str(item_row.get("step_id")),
                        deliverable_id=str(item_row.get("deliverable_id")),
                        feature=str(item_row.get("feature")),
                        type=str(item_row.get("type")),
                        amount_credits=int(str(item_row.get("amount_credits", "0") or "0").strip() or "0"),
                        created_at=str(item_row.get("created_at")),
                        note=str(item_row.get("note" ) or ""),
                        metadata_json=str(item_row.get("metadata_json") or "{}"),
                    ))
                except Exception:
                    pass


            # Run spend (deliverable_id="__run__")
            run_p = int(breakdown.get("__run__", 0))
            if run_p:
                idem = key_step_run_charge(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
                meta = {"step_id": sid, "step_name": sname, "deliverable_id": "__run__", "requested_deliverables": req_deliverables, "deliverables_source": del_src, "idempotency_key": idem}
                if mid in ("deliver_email", "deliver_dropbox"):
                    provider = mid.replace("deliver_", "")
                    meta.update({
                        "provider": provider,
                        "remote_object_id": "",
                        "remote_path": "",
                        "verification_status": "unverified",
                    })
                _append_tx_item({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "work_order_id": work_order_id,
                    "step_id": sid,
                    "deliverable_id": "__run__",
                    "feature": "__run__",
                    "type": "SPEND",
                    "amount_credits": str(-run_p),
                    "created_at": utcnow_iso(),
                    "note": f"Run spend: {m_label}",
                    "metadata_json": json.dumps(meta, separators=(",", ":")),
                })

            # Deliverable spend per purchased deliverable_id
            for did in req_deliverables:
                ds = str(did or "").strip()
                if not ds or ds == "__run__":
                    continue
                p = int(breakdown.get(ds, 0))
                if p <= 0:
                    continue
                idem = key_deliverable_charge(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid, deliverable_id=ds)
                meta = {"step_id": sid, "step_name": sname, "deliverable_id": ds, "requested_deliverables": req_deliverables, "deliverables_source": del_src, "idempotency_key": idem}
                _append_tx_item({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "work_order_id": work_order_id,
                    "step_id": sid,
                    "deliverable_id": ds,
                    "feature": ds,
                    "type": "SPEND",
                    "amount_credits": str(-p),
                    "created_at": utcnow_iso(),
                    "note": f"Deliverable spend ({ds}): {m_label}",
                    "metadata_json": json.dumps(meta, separators=(",", ":")),
                })

        # update balance
        trow["credits_available"] = str(available - est_total)
        trow["updated_at"] = utcnow_iso()

        mode = str(w.get("mode","")).strip().upper() or "PARTIAL_ALLOWED"
        any_failed = False
        completed_steps: List[str] = []
        completed_modules: List[str] = []
        step_outputs: Dict[str, Path] = {}

        # Ports (tenant-visible vs platform-only) and output exposure rules.
        ports_cache: Dict[str, Dict[str, Any]] = {}
        step_allowed_outputs: Dict[str, Set[str]] = {}
        for st in plan:
            st_step_id = str(st.get("step_id") or "").strip()
            st_module_id = canon_module_id(st.get("module_id") or "")
            if not st_step_id or not st_module_id:
                continue
            if st_module_id not in ports_cache:
                ports_cache[st_module_id] = _load_module_ports(registry, st_module_id)
            _t_in, _p_in, _t_out = _ports_index(ports_cache[st_module_id])
            step_allowed_outputs[st_step_id] = set(_t_out)

        # Execute steps (modules-only workorders and steps-based chaining workorders)
        for step in plan:
            sid = str(step.get("step_id") or "").strip()
            mid = canon_module_id(step.get("module_id") or "")
            cfg = dict(step.get("cfg") or {})
            m_started = utcnow_iso()
            m_started = utcnow_iso()

            step_run_idem = key_step_run(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
            step_run = run_state.create_step_run(
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                step_id=sid,
                module_id=mid,
                idempotency_key=step_run_idem,
                outputs_dir=runtime_dir / 'runs' / tenant_id / work_order_id / sid,
                metadata={'plan_type': plan_type, 'step_name': str((cfg.get('step_name') or cfg.get('name') or '')).strip()},
            )
            mr_id = step_run.module_run_id

            requested_deliverables = per_step_requested_deliverables.get(sid, []) or []
            deliverables_source = per_step_deliverables_source.get(sid, "none")
            applied_limited_inputs: Dict[str, Any] = {}
            effective_inputs_hash = ""

            # Resolve step inputs (supports bindings: {from_step, from_file, selector, json_path, take}).
            # Enforce module ports: tenants can only set tenant-visible inputs; platform-only inputs are injected via defaults.
            inputs_spec = cfg.get("inputs") or {}
            try:
                if not isinstance(inputs_spec, dict):
                    raise ValueError("step.inputs must be an object")

                if mid not in ports_cache:
                    ports_cache[mid] = _load_module_ports(registry, mid)
                tenant_inputs, platform_inputs, _tenant_out = _ports_index(ports_cache[mid])

                # Apply deliverables-driven platform-only inputs (limited_port).
                # Tenant inputs are merged with derived limited_inputs; derived values override on collision.
                if requested_deliverables:
                    contract = deliverables_cache.get(mid)
                    if contract is None:
                        try:
                            _c = registry.get_contract(mid)
                        except Exception:
                            _c = {}
                        _d = _c.get('deliverables') or {}
                        if not isinstance(_d, dict):
                            _d = {}
                        contract = {}
                        for _did, _dd in _d.items():
                            if not isinstance(_dd, dict):
                                continue
                            contract[str(_did)] = {
                                'limited_inputs': dict(_dd.get('limited_inputs') or {}),
                                'output_paths': list(_dd.get('output_paths') or []),
                            }
                        deliverables_cache[mid] = contract
                    applied_limited_inputs = _union_limited_inputs(contract, requested_deliverables)
                    for k in applied_limited_inputs.keys():
                        if k in tenant_inputs:
                            raise PermissionError(f"Deliverable limited_input '{k}' must not be a tenant input for module {mid}")
                        if k not in platform_inputs:
                            raise KeyError(f"Deliverable limited_input '{k}' is not declared as limited_port for module {mid}")

                if tenant_inputs or platform_inputs:
                    # Reject any attempt to set platform-only inputs.
                    for k in inputs_spec.keys():
                        if k in platform_inputs:
                            raise PermissionError(f"Input '{k}' is platform-only for module {mid}")
                        if k not in tenant_inputs:
                            raise KeyError(f"Unknown input '{k}' for module {mid}")

                    # Inject defaults (tenant + platform) before binding resolution.
                    merged_spec: Dict[str, Any] = dict(inputs_spec)
                    for pid, pspec in tenant_inputs.items():
                        if pid not in merged_spec and "default" in pspec:
                            merged_spec[pid] = pspec.get("default")
                    for pid, pspec in platform_inputs.items():
                        if pid not in merged_spec and "default" in pspec:
                            merged_spec[pid] = pspec.get("default")

                    # Deliverables may request platform-only flags; these override tenant inputs and defaults.
                    for k, v in (applied_limited_inputs or {}).items():
                        merged_spec[k] = v

                    resolved_inputs = _resolve_inputs(merged_spec, step_outputs, step_allowed_outputs, run_state, tenant_id, work_order_id)

                    # Required tenant inputs must be present and non-empty after resolution.
                    for pid, pspec in tenant_inputs.items():
                        if not bool(pspec.get("required", False)):
                            continue
                        if pid not in resolved_inputs:
                            raise ValueError(f"Missing required input '{pid}' for module {mid}")
                        v = resolved_inputs.get(pid)
                        if v is None or (isinstance(v, str) and not v.strip()):
                            raise ValueError(f"Missing required input '{pid}' for module {mid}")
                else:
                    # Legacy permissive behavior: if module has no ports, accept any inputs.
                    resolved_inputs = _resolve_inputs(inputs_spec, step_outputs, step_allowed_outputs, run_state, tenant_id, work_order_id)

                resolve_error = ""
            except Exception as e:
                resolved_inputs = {}
                resolve_error = str(e)

            if not resolve_error:
                effective_inputs_hash = _effective_inputs_hash(resolved_inputs)

            sname = str(cfg.get("step_name") or cfg.get("name") or "").strip()

            params: Dict[str, Any] = {
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "module_run_id": mr_id,
                "inputs": resolved_inputs,
                "reuse_output_type": str(cfg.get("reuse_output_type","")).strip(),
                "_platform": {"plan_type": plan_type, "step_id": sid, "step_name": sname, "module_id": mid, "run_id": spend_tx},
            }
            # Backward compatibility: also expose resolved inputs at top-level (without overriding reserved keys).
            if isinstance(resolved_inputs, dict):
                for k, v in resolved_inputs.items():
                    if k not in params and k not in ("inputs", "_platform"):
                        params[k] = v

            module_path = repo_root / "modules" / mid
            out_dir = runtime_dir / "runs" / tenant_id / work_order_id / sid / mr_id
            ensure_dir(out_dir)

            step_run = run_state.mark_step_run_running(mr_id, metadata={'outputs_dir': str(out_dir)})

            # ------------------------------------------------------------------
            # Performance cache: reuse module outputs from runtime/cache_outputs
            # when reuse_output_type == "cache".
            # ------------------------------------------------------------------
            reuse_type = str(cfg.get("reuse_output_type", "")).strip().lower()
            key_inputs = resolved_inputs if isinstance(resolved_inputs, dict) else {}
            cache_key = derive_cache_key(module_id=mid, tenant_id=tenant_id, key_inputs=key_inputs)
            cache_dir = cache_root / _cache_dirname(cache_key)

            cache_row = None
            for r in cache_index:
                if (str(r.get('place','')).strip() == 'cache'
                    and str(r.get('type','')).strip() == 'module_run'
                    and str(r.get('ref','')).strip() == cache_key):
                    cache_row = r
                    break

            cache_valid = False
            if cache_row is not None:
                try:
                    exp = _parse_iso_z(str(cache_row.get("expires_at", "")))
                    cache_valid = exp > datetime.now(timezone.utc)
                except Exception:
                    cache_valid = False
            if resolve_error:
                # Chaining input resolution failed; do not execute the module.
                report = out_dir / "binding_error.json"
                report.write_text(
                    json.dumps(
                        {
                            'step_id': sid,
                            'module_id': mid,
                            'error': resolve_error,
                            'inputs_spec': cfg.get('inputs') or {},
                        },
                        indent=2,
                    )
                    + '\n',
                    encoding='utf-8',
                )
                err = {'reason_code': 'missing_required_input', 'message': resolve_error, 'type': 'BindingResolutionError'}
                step_run = run_state.mark_step_run_failed(mr_id, err)
                result = {
                    'status': 'FAILED',
                    'reason_slug': 'missing_required_input',
                    'report_path': 'binding_error.json',
                    'output_ref': '',
                }
            elif reuse_type == "cache" and _dir_has_files(cache_dir) and (cache_row is None or cache_valid):
                _copy_tree(cache_dir, out_dir)
                result = {
                    "status": "COMPLETED",
                    "reason_slug": "",
                    "report_path": "",
                    "output_ref": f"cache:{cache_key}",
                    "_cache_hit": True,
                }
            else:
                module_env = env_for_module(store, mid)
                result = execute_module_runner(module_path=module_path, params=params, outputs_dir=out_dir, env=module_env)

            # Record outputs into RunStateStore using module contract output paths (latest wins).
            if str(result.get('status','') or '').upper() == 'COMPLETED':
                try:
                    contract = registry.get_contract(mid)
                except Exception:
                    contract = {}
                outputs_def = contract.get('outputs') or {}
                module_kind = str(contract.get('kind') or 'transform').strip() or 'transform'
                if isinstance(outputs_def, dict):
                    for output_id, odef in outputs_def.items():
                        if not isinstance(odef, dict):
                            continue
                        rel_path = str(odef.get('path') or '').lstrip('/').strip()
                        if not rel_path:
                            continue
                        abs_path = out_dir / rel_path
                        if not abs_path.exists():
                            continue
                        try:
                            from platform.utils.hashing import sha256_file
                            sha = sha256_file(abs_path)
                            bs = int(abs_path.stat().st_size)
                        except Exception:
                            sha = ''
                            bs = 0
                        try:
                            from platform.infra.models import OutputRecord
                            rec = OutputRecord(
                                tenant_id=tenant_id,
                                work_order_id=work_order_id,
                                step_id=sid,
                                module_id=mid,
                                kind=module_kind,
                                output_id=str(output_id),
                                path=rel_path,
                                uri=abs_path.resolve().as_uri(),
                                content_type=str(odef.get('format') or ''),
                                sha256=sha,
                                bytes=bs,
                                bytes_size=bs,
                                created_at=utcnow_iso(),
                            )
                            run_state.record_output(rec)
                        except Exception:
                            pass
                step_run = run_state.mark_step_run_succeeded(
                    mr_id,
                    requested_deliverables=list(requested_deliverables or []),
                    metadata={'outputs_dir': str(out_dir)},
                )
            else:
                if str(result.get('status','') or '').upper() == 'FAILED':
                    # Prefer canonical reason_code (from reason_catalog) in run-state logs.
                    _rs = str(result.get('reason_slug') or result.get('reason_key') or 'module_failed').strip() or 'module_failed'
                    _rc = _reason_code(reason_idx, "MODULE", mid, _rs) or _reason_code(reason_idx, "GLOBAL", "", _rs) or ""
                    err = {'reason_code': _rc or _rs, 'message': 'module failed', 'type': 'ModuleFailed'}
                    step_run = run_state.mark_step_run_failed(mr_id, err)

            raw_status = str(result.get("status", "") or "").strip()
            if raw_status:
                status = raw_status.upper()
            else:
                files = result.get("files")
                status = "COMPLETED" if isinstance(files, list) else "FAILED"

            reason_slug = str(result.get("reason_slug", "") or "").strip() or str(result.get("reason_key", "") or "").strip()
            if status == "COMPLETED":
                completed_steps.append(sid)
                completed_modules.append(mid)
                reason_code = ""
            else:
                any_failed = True
                if not reason_slug:
                    reason_slug = "module_failed"
                reason_code = _reason_code(reason_idx, "MODULE", mid, reason_slug) or _reason_code(reason_idx, "GLOBAL", "", reason_slug) or _reason_code(reason_idx, "GLOBAL", "", "module_failed")

            # output ref / report path: optional
            report_path = str(result.get("report_path","") or "")
            output_ref = str(result.get("output_ref","") or "")

            cache_hit = bool(result.get("_cache_hit", False))

            module_runs_log.append({
                "module_run_id": mr_id,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "module_id": mid,
                "status": status,
                "created_at": created_at,
                "started_at": m_started,
                "ended_at": utcnow_iso(),
                "reason_code": reason_code,
                "report_path": report_path,
                "output_ref": output_ref,
                "metadata_json": json.dumps({"plan_type": plan_type, "step_id": sid, "step_name": sname, "outputs_dir": str(out_dir), "cache_key": cache_key, "cache_hit": cache_hit, "requested_deliverables": requested_deliverables, "deliverables_source": deliverables_source, "applied_limited_inputs": applied_limited_inputs, "effective_inputs_hash": effective_inputs_hash}, separators=(",", ":")),
            })

            # Make outputs discoverable for downstream bindings (even if the step failed).
            if sid:
                step_outputs[sid] = out_dir

            # Delivery evidence line-item (zero-credit) for reporting.
            # This keeps audit metadata (provider, remote_path, verification, bytes) in the ledger
            # without mutating the original __run__ charge row.
            if status == "COMPLETED" and (str(step.get("kind") or "").strip() == "delivery" or module_kind == "delivery"):
                receipt_path = out_dir / "delivery_receipt.json"
                if receipt_path.exists():
                    try:
                        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                    except Exception:
                        receipt = {}
                    provider = str(receipt.get("provider") or "").strip()
                    remote_path_ev = str(receipt.get("remote_path") or "").strip()
                    remote_object_id_ev = str(receipt.get("remote_object_id") or "").strip()
                    verification_status_ev = str(receipt.get("verification_status") or "").strip()
                    bytes_ev = int(str(receipt.get("bytes") or "0").strip() or "0")
                    sha256_ev = str(receipt.get("sha256") or "").strip()
                    idem_ev = key_delivery_evidence(tenant_id=tenant_id, work_order_id=work_order_id, step_id=sid, module_id=mid)
                    try:
                        receipt_rel = str(receipt_path.relative_to(repo_root)).replace("\\", "/")
                    except Exception:
                        receipt_rel = str(receipt_path)

                    ev_meta = {
                        "idempotency_key": idem_ev,
                        "step_id": sid,
                        "step_name": sname,
                        "module_id": mid,
                        "provider": provider,
                        "remote_path": remote_path_ev,
                        "remote_object_id": remote_object_id_ev,
                        "verification_status": verification_status_ev,
                        "bytes": bytes_ev,
                        "sha256": sha256_ev,
                        "receipt_path": receipt_rel,
                    }
                    already = False
                    for existing in transaction_items:
                        try:
                            em = json.loads(str(existing.get("metadata_json") or "{}")).get("idempotency_key")
                        except Exception:
                            em = ""
                        if str(em) == idem_ev:
                            already = True
                            break
                    if not already:
                        ev_row = {
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": spend_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "work_order_id": work_order_id,
                            "step_id": sid,
                            "deliverable_id": "__delivery_evidence__",
                            "feature": "delivery_evidence",
                            "type": "SPEND",
                            "amount_credits": "0",
                            "created_at": utcnow_iso(),
                            "note": f"Delivery evidence: {_label(mid, sid, sname)}",
                            "metadata_json": json.dumps(ev_meta, separators=(",", ":")),
                        }
                        transaction_items.append(ev_row)
                        try:
                            ledger.post_transaction_item(TransactionItemRecord(
                                transaction_item_id=str(ev_row.get("transaction_item_id")),
                                transaction_id=str(ev_row.get("transaction_id")),
                                tenant_id=str(ev_row.get("tenant_id")),
                                module_id=str(ev_row.get("module_id")),
                                work_order_id=str(ev_row.get("work_order_id")),
                                step_id=str(ev_row.get("step_id")),
                                deliverable_id=str(ev_row.get("deliverable_id")),
                                feature=str(ev_row.get("feature")),
                                type=str(ev_row.get("type")),
                                amount_credits=int(str(ev_row.get("amount_credits") or "0")),
                                created_at=str(ev_row.get("created_at")),
                                note=str(ev_row.get("note") or ""),
                                metadata_json=str(ev_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

            # Persist successful outputs into the local module cache.
            # Cache is only reused when reuse_output_type == "cache".
            if status == "COMPLETED":
                if not cache_hit:
                    _copy_tree(out_dir, cache_dir)

                now_dt = datetime.now(timezone.utc).replace(microsecond=0)
                exp_dt = now_dt + timedelta(days=int(cache_ttl_days))
                if cache_row is None:
                    cache_index.append({
                        'place': 'cache',
                        'type': 'module_run',
                        'ref': cache_key,
                        'created_at': now_dt.isoformat().replace('+00:00', 'Z'),
                        'expires_at': exp_dt.isoformat().replace('+00:00', 'Z'),
                    })
                else:
                    # Extend expiry forward if needed; keep created_at stable.
                    try:
                        old_exp = _parse_iso_z(str(cache_row.get('expires_at', '')))
                    except Exception:
                        old_exp = datetime(1970, 1, 1, tzinfo=timezone.utc)
                    if exp_dt > old_exp:
                        cache_row['expires_at'] = exp_dt.isoformat().replace('+00:00', 'Z')


                # Persist cache_index.csv after any mutation so cache entries are durable even if later steps fail.
                try:
                    billing.save_table("cache_index.csv", cache_index, headers=CACHE_INDEX_HEADERS)
                except Exception as e:
                    print(f"[cache_index][WARN] failed to persist cache_index.csv mid-run: {e}")


            # Refund policy
            # - Refund reasons are governed by reason_catalog.csv (refundable=true)
            # - For delivery steps, refund is only allowed when the module returns refund_eligible=true,
            #   which means the module has verified non-delivery (or the failure is deterministic).
            step_kind = str((step.get("kind") or cfg.get("kind") or "")).strip()
            is_delivery_step = (step_kind == "delivery" or module_kind == "delivery")
            refund_eligible = bool(result.get("refund_eligible", False))
            refundable = bool(reason_idx.refundable.get(reason_code, False))
            if is_delivery_step:
                refundable = refundable and refund_eligible

            # IMPORTANT: refunds must be itemized to mirror spend line-items (__run__ + deliverables).
            if status != "COMPLETED" and reason_code and refundable:
                # Prefer the itemized parts captured at spend time. If missing for any reason,
                # recompute from pricing so refunds are always recorded and itemized.
                breakdown = per_step_prices.get(sid)
                if breakdown is None:
                    breakdown = _price_breakdown_for_step(prices, mid, per_step_requested_deliverables.get(sid, []) or [])
                refund_amt = _sum_prices(breakdown)
                if refund_amt > 0:

                    m_label = _label(mid, sid, sname)

                    # Create an idempotent refund transaction keyed off the step + reason.
                    refund_tx_idem = "tx_" + key_refund(
                        tenant_id=tenant_id,
                        work_order_id=work_order_id,
                        step_id=sid,
                        module_id=mid,
                        deliverable_id="__run__",
                        reason_key=reason_code,
                    )

                    refund_tx = ""
                    for tx in transactions:
                        try:
                            meta = json.loads(str(tx.get("metadata_json") or "{}")) if str(tx.get("metadata_json") or "").strip() else {}
                        except Exception:
                            meta = {}
                        if str(meta.get("idempotency_key") or "") == refund_tx_idem:
                            refund_tx = str(tx.get("transaction_id") or "")
                            break

                    now = utcnow_iso()

                    if not refund_tx:
                        refund_tx = _new_id("transaction_id", used_tx)
                        tx_meta = {
                            "step_id": sid,
                            "step_name": sname,
                            "module_id": mid,
                            "refund_for": mr_id,
                            "spend_transaction_id": spend_tx,
                            "idempotency_key": refund_tx_idem,
                        }
                        tx_row = {
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "work_order_id": work_order_id,
                            "type": "REFUND",
                            "amount_credits": str(refund_amt),
                            "created_at": now,
                            "reason_code": reason_code,
                            "note": f"Refund: {m_label} (reason={reason_code})",
                            "metadata_json": json.dumps(tx_meta, separators=(",", ":")),
                        }
                        transactions.append(tx_row)
                        try:
                            ledger.post_transaction(TransactionRecord(
                                transaction_id=refund_tx,
                                tenant_id=tenant_id,
                                work_order_id=work_order_id,
                                type="REFUND",
                                amount_credits=int(refund_amt),
                                created_at=now,
                                reason_code=reason_code,
                                note=str(tx_row.get("note") or ""),
                                metadata_json=str(tx_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

                    # Refund items per priced deliverable_id (including __run__), idempotent.
                    for did, amt in sorted((breakdown or {}).items(), key=lambda kv: kv[0]):
                        a = int(amt)
                        if a <= 0:
                            continue

                        item_idem = key_refund(
                            tenant_id=tenant_id,
                            work_order_id=work_order_id,
                            step_id=sid,
                            module_id=mid,
                            deliverable_id=str(did),
                            reason_key=reason_code,
                        )
                        # Do not duplicate refund items on rerun.
                        duplicate = False
                        for existing in transaction_items:
                            try:
                                em = json.loads(str(existing.get("metadata_json") or "{}")) if str(existing.get("metadata_json") or "").strip() else {}
                            except Exception:
                                em = {}
                            if str(em.get("idempotency_key") or "") == item_idem:
                                duplicate = True
                                break
                        if duplicate:
                            continue

                        item_meta = {
                            "step_id": sid,
                            "step_name": sname,
                            "module_id": mid,
                            "refund_for": mr_id,
                            "deliverable_id": str(did),
                            "spend_transaction_id": spend_tx,
                            "idempotency_key": item_idem,
                        }
                        item_row = {
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "work_order_id": work_order_id,
                            "step_id": sid,
                            "deliverable_id": str(did),
                            "feature": str(did),
                            "type": "REFUND",
                            "amount_credits": str(a),
                            "created_at": now,
                            "note": f"Refund item ({did}): {m_label} (reason={reason_code})",
                            "metadata_json": json.dumps(item_meta, separators=(",", ":")),
                        }
                        transaction_items.append(item_row)
                        try:
                            ledger.post_transaction_item(TransactionItemRecord(
                                transaction_item_id=str(item_row.get("transaction_item_id")),
                                transaction_id=str(item_row.get("transaction_id")),
                                tenant_id=str(item_row.get("tenant_id")),
                                module_id=str(item_row.get("module_id")),
                                work_order_id=str(item_row.get("work_order_id")),
                                step_id=str(item_row.get("step_id")),
                                deliverable_id=str(item_row.get("deliverable_id")),
                                feature=str(item_row.get("feature")),
                                type=str(item_row.get("type")),
                                amount_credits=int(str(item_row.get("amount_credits") or "0")),
                                created_at=str(item_row.get("created_at")),
                                note=str(item_row.get("note") or ""),
                                metadata_json=str(item_row.get("metadata_json") or "{}"),
                            ))
                        except Exception:
                            pass

                    # balance update

                    trow["credits_available"] = str(int(trow["credits_available"]) + refund_amt)
                    trow["updated_at"] = utcnow_iso()

            # Deliverables publication is handled as a reconciliation step by scripts/publish_artifacts_release.py
            # Orchestrator only records requested deliverables and runs modules; it does not publish artifacts.
            if status != "COMPLETED" and mode == "ALL_OR_NOTHING":
                break

        ended_at = utcnow_iso()

        # Canonical status semantics
        step_statuses = {}
        for row in module_runs_log:
            if str(row.get("tenant_id")) != tenant_id or str(row.get("work_order_id")) != work_order_id:
                continue
            try:
                meta = json.loads(str(row.get("metadata_json") or "{}")) if str(row.get("metadata_json") or "").strip() else {}
            except Exception:
                meta = {}
            sid_from_meta = str(meta.get("step_id") or "").strip()
            if sid_from_meta:
                step_statuses[sid_from_meta] = str(row.get("status") or "").strip().upper()

        purchased_deliverables_by_step = {
            sid: (per_step_requested_deliverables.get(sid) or [])
            for sid in per_step_requested_deliverables.keys()
            if (per_step_requested_deliverables.get(sid) or [])
        }
        refunds_exist = any(
            str(r.get("tenant_id")) == tenant_id and str(r.get("work_order_id")) == work_order_id and str(r.get("type")) == "REFUND"
            for r in transaction_items
        )

        publish_required = bool(purchased_deliverables_by_step)
        publish_completed = False
        reduced = reduce_workorder_status(StatusInputs(
            step_statuses=step_statuses,
            refunds_exist=refunds_exist,
            publish_required=publish_required,
            publish_completed=publish_completed,
        ))
        awaiting_publish = (reduced == "AWAITING_PUBLISH")
        final_status = "PARTIAL" if awaiting_publish else reduced

        print(
            f"[orchestrator] work_order_id={work_order_id} status={final_status} plan_type={plan_type} "
            f"completed_steps={completed_steps}"
        )

        note = f"{final_status}: {plan_human}"
        if awaiting_publish:
            note = f"PARTIAL: AWAITING_PUBLISH: {plan_human}"

        ctx = OrchestratorContext(tenant_id=tenant_id, work_order_id=work_order_id, run_id=spend_tx, runtime_profile_name=runtime_profile_name)
        try:
            run_state.set_run_status(
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                status=final_status,
                metadata={
                    "plan_type": plan_type,
                    "run_id": ctx.run_id,
                    "runtime_profile_name": ctx.runtime_profile_name,
                    "awaiting_publish": awaiting_publish,
                    "purchased_deliverables_by_step": purchased_deliverables_by_step,
                },
            )
        except Exception:
            pass

        workorders_log.append({
            "work_order_id": work_order_id,
            "tenant_id": tenant_id,
            "status": final_status,
            "created_at": created_at,
            "started_at": started_at,
            "ended_at": ended_at,
            "note": note,
            "metadata_json": json.dumps(
                {
                    "plan_type": plan_type,
                    "requested_steps": [p.get("step_id") for p in plan],
                    "requested_modules": [p.get("module_id") for p in plan],
                    "completed_steps": completed_steps,
                    "completed_modules": completed_modules,
                    "any_failed": any_failed,
                    "awaiting_publish": awaiting_publish,
                    "purchased_deliverables_by_step": purchased_deliverables_by_step,
                },
                separators=(",", ":"),
            ),
        })

    # Persist cache_index.csv updates (required for durable cache behavior across runs).
    # This is intentionally scoped to cache_index only: other billing-state tables are
    # written via the ledger/runstate adapters.
    try:
        billing.save_table("cache_index.csv", cache_index, headers=CACHE_INDEX_HEADERS)
    except Exception as e:
        print(f"[cache_index][WARN] failed to persist cache_index.csv: {e}")

    # Adapter mode: orchestrator no longer persists billing-state tables directly.
    # LedgerWriter and RunStateStore are the only write paths.
