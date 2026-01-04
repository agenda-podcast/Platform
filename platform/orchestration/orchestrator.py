from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from ..billing.state import BillingState
from ..common.id_codec import canon_module_id, canon_tenant_id, canon_work_order_id, id_key, dedupe_tenants_credits
from ..common.id_policy import generate_unique_id, validate_id
from ..github.releases import ensure_release, upload_release_assets, get_release_numeric_id, get_release_assets_numeric_ids
from ..orchestration.module_exec import execute_module_runner
from ..utils.csvio import read_csv
from ..utils.fs import ensure_dir
from ..utils.hashing import sha256_bytes, sha256_file, short_hash
from ..utils.time import utcnow_iso


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
    "transaction_item_id","transaction_id","tenant_id","module_id","feature","type","amount_credits","created_at","note","metadata_json",
]

TENANTS_CREDITS_HEADERS = ["tenant_id","credits_available","updated_at","status"]

CACHE_INDEX_HEADERS = ["cache_key","tenant_id","module_id","created_at","expires_at","cache_id"]

PROMOTION_REDEMPTIONS_HEADERS = ["redemption_id","tenant_id","promo_code","credits_granted","created_at","note","metadata_json"]

GITHUB_RELEASES_MAP_HEADERS = ["release_id","github_release_id","tag","tenant_id","work_order_id","created_at"]

GITHUB_ASSETS_MAP_HEADERS = ["asset_id","github_asset_id","release_id","asset_name","created_at"]


@dataclass(frozen=True)
class ReasonIndex:
    by_key: Dict[Tuple[str, str, str], str]  # (scope, module_id, reason_slug) -> reason_code
    refundable: Dict[str, bool]              # reason_code -> refundable
    description: Dict[str, str]              # reason_code -> human description


def _repo_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


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

    description: Dict[str, str] = {}
    for r in catalog:
        code = str(r.get("reason_code", "")).strip()
        if not code:
            continue
        desc = str(r.get("description", "")).strip()
        if desc:
            description[code] = desc

    return ReasonIndex(by_key=by_key, refundable=refundable, description=description)


def _load_module_display_names(repo_root: Path) -> Dict[str, str]:
    """module_id -> display name (maintenance-state/ids/module_registry.csv)."""
    rows = read_csv(repo_root / "maintenance-state" / "ids" / "module_registry.csv")
    out: Dict[str, str] = {}
    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        name = str(r.get("display_name", "")).strip()
        if name:
            out[mid] = name
    return out


def _reason_code(idx: ReasonIndex, scope: str, module_id: str, reason_slug: str) -> str:
    scope_u = scope.upper()
    mid = module_id if scope_u == "MODULE" else ""
    code = idx.by_key.get((scope_u, mid, reason_slug))
    if code:
        return code
    # fallback: global unknown_error if present
    code = idx.by_key.get(("GLOBAL", "", "unknown_error"))
    return code or ""


def _load_module_prices(repo_root: Path) -> Dict[str, Dict[str, str]]:
    rows = read_csv(repo_root / "platform" / "billing" / "module_prices.csv")
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        if not mid:
            continue
        out[mid] = r
    return out


def _price_for_module(prices: Dict[str, Dict[str, str]], module_id: str, purchase_release_artifacts: bool) -> int:
    row = prices.get(module_id)
    if not row:
        return 0
    try:
        run_price = int(str(row.get("price_run_credits", "0")).strip() or "0")
        rel_price = int(str(row.get("price_save_to_release_credits", "0")).strip() or "0")
    except Exception:
        return 0
    return run_price + (rel_price if purchase_release_artifacts else 0)

def _module_line_items(
    prices: Dict[str, Dict[str, str]],
    module_id: str,
    purchase_release_artifacts: bool,
) -> List[Tuple[str, int]]:
    """Return billable line-items for a module as (feature, amount_credits_positive)."""
    row = prices.get(module_id) or {}
    try:
        run_price = int(str(row.get("price_run_credits", "0")).strip() or "0")
        rel_price = int(str(row.get("price_save_to_release_credits", "0")).strip() or "0")
    except Exception:
        return []

    items: List[Tuple[str, int]] = []
    if run_price > 0:
        items.append(("RUN", run_price))
    if purchase_release_artifacts and rel_price > 0:
        items.append(("SAVE_ARTIFACTS", rel_price))
    return items


def _module_label(module_id: str, module_names: Dict[str, str]) -> str:
    name = module_names.get(module_id, "").strip()
    return f"{module_id} - {name}" if name else module_id


def _note_for_spend_item(feature: str, module_lbl: str) -> str:
    if feature == "RUN":
        return f"Run module {module_lbl}"
    if feature == "SAVE_ARTIFACTS":
        return f"Save artifacts from module {module_lbl}"
    return f"Charge {feature} for module {module_lbl}"


def _note_for_refund_item(feature: str, module_lbl: str, reason_code: str, reason_desc: str) -> str:
    reason = f"{reason_code} - {reason_desc}" if reason_desc else reason_code
    if feature == "RUN":
        return f"Failed run module {module_lbl}: {reason}"
    if feature == "SAVE_ARTIFACTS":
        return f"Failed save artifacts from module {module_lbl}: {reason}"
    return f"Refund {feature} for module {module_lbl}: {reason}"


def _toposort_modules(modules_requested: List[str], deps_index: List[Dict[str, str]]) -> List[str]:
    requested = [canon_module_id(m) for m in modules_requested if canon_module_id(m)]
    requested_set = set(requested)

    deps: Dict[str, Set[str]] = {m: set() for m in requested}
    for r in deps_index:
        mid = canon_module_id(r.get("module_id", ""))
        dep = canon_module_id(r.get("depends_on_module_id", ""))
        if not (mid and dep):
            continue
        if mid in requested_set and dep in requested_set:
            deps[mid].add(dep)

    ordered: List[str] = []
    temp: Set[str] = set()
    perm: Set[str] = set()

    def visit(n: str) -> None:
        if n in perm:
            return
        if n in temp:
            raise ValueError(f"Cycle in module dependencies at {n}")
        temp.add(n)
        for d in sorted(deps.get(n, set())):
            visit(d)
        temp.remove(n)
        perm.add(n)
        ordered.append(n)

    for m in requested:
        visit(m)
    return ordered


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

def _load_module_spec(module_path: Path) -> Dict[str, Any]:
    cfg = _repo_yaml(module_path / "module.yml")
    if not isinstance(cfg, dict):
        return {}
    return cfg


def _module_ports(spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ports = spec.get("ports") or {}
    in_ports = ports.get("inputs") or spec.get("inputs") or []
    out_ports = ports.get("outputs") or spec.get("outputs") or []
    if not isinstance(in_ports, list):
        in_ports = []
    if not isinstance(out_ports, list):
        out_ports = []
    return in_ports, out_ports


def _find_port(ports: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    key = str(name or "").strip()
    for p in ports:
        if str((p or {}).get("name","")) == key:
            return p
    return None


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _read_json_file(path: Path) -> Any:
    return json.loads(_read_text_file(path))


def _read_jsonl_first(path: Path) -> Any:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for ln in f:
            s = ln.strip()
            if not s:
                continue
            return json.loads(s)
    return None


def _resolve_selector_value(
    selector: Dict[str, Any],
    *,
    tenant_id: str,
    repo_root: Path,
    runtime_dir: Path,
    artifacts_by_step: Dict[str, Dict[str, Path]],
    expected_format: str,
) -> List[Any]:
    src = str(selector.get("from") or "").strip().upper()
    extract = str(selector.get("extract") or "").strip().lower()

    def apply_extract(p: Path) -> Any:
        fmt = (expected_format or "").strip().lower()
        ex = extract or ("json" if "json" in fmt else "text")
        if ex in ("text","raw"):
            return _read_text_file(p)
        if ex == "json":
            return _read_json_file(p)
        if ex == "jsonl_first":
            return _read_jsonl_first(p)
        if ex == "path":
            return str(p)
        # default fallback
        return _read_text_file(p)

    values: List[Any] = []

    if src == "NEW":
        step_id = str(selector.get("step_id") or "").strip()
        out_name = str(selector.get("output") or "").strip()
        p = (artifacts_by_step.get(step_id) or {}).get(out_name)
        if p and p.exists():
            values.append(apply_extract(p))
        return values

    if src == "ASSET":
        rel_path = str(selector.get("path") or "").strip()
        if rel_path:
            p = repo_root / "tenants" / tenant_id / "assets" / rel_path
            if p.exists():
                values.append(apply_extract(p))
        return values

    if src == "RELEASE":
        # Offline implementation: resolve from runtime_dir/releases/<release_id>/...
        release_id = str(selector.get("release_id") or "").strip()
        rel_path = str(selector.get("path") or "").strip()
        if release_id and rel_path:
            p = runtime_dir / "releases" / release_id / rel_path
            if p.exists():
                values.append(apply_extract(p))
        return values

    if src == "CACHE":
        # Offline implementation: resolve from runtime_dir/cache/<cache_id>/...
        cache_id = str(selector.get("cache_id") or "").strip()
        rel_path = str(selector.get("path") or "").strip()
        if cache_id and rel_path:
            p = runtime_dir / "cache" / cache_id / rel_path
            if p.exists():
                values.append(apply_extract(p))
        return values

    return values


def _resolve_binding(
    binding: Any,
    *,
    tenant_id: str,
    repo_root: Path,
    runtime_dir: Path,
    artifacts_by_step: Dict[str, Dict[str, Path]],
    expected_format: str,
    expected_cardinality: str,
) -> Any:
    """Resolve a workorder input binding.

    Binding can be:
      - literal scalar/list/dict (returned as-is)
      - dict with {mode, selectors:[...]}

    Cardinality is used only to normalize selector outputs.
    """

    if not isinstance(binding, dict) or "selectors" not in binding:
        return binding

    mode = str(binding.get("mode") or ("list" if expected_cardinality == "list" else "single")).strip().lower()
    selectors = binding.get("selectors") or []
    if not isinstance(selectors, list):
        selectors = []

    gathered: List[Any] = []
    for sel in selectors:
        if not isinstance(sel, dict):
            continue
        vals = _resolve_selector_value(
            sel,
            tenant_id=tenant_id,
            repo_root=repo_root,
            runtime_dir=runtime_dir,
            artifacts_by_step=artifacts_by_step,
            expected_format=expected_format,
        )
        for v in vals:
            if isinstance(v, list) and mode in ("list","extend"):
                gathered.extend(v)
            else:
                gathered.append(v)

    if mode in ("list","extend"):
        return gathered
    # single / first
    return gathered[0] if gathered else None


def _toposort_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Toposort based on NEW selectors referencing previous step outputs."""
    by_id: Dict[str, Dict[str, Any]] = {}
    for s in steps:
        sid = str((s or {}).get("step_id") or "").strip()
        if sid:
            by_id[sid] = s

    deps: Dict[str, Set[str]] = {sid: set() for sid in by_id.keys()}
    for sid, s in by_id.items():
        inputs = (s or {}).get("inputs") or {}
        if not isinstance(inputs, dict):
            continue
        for v in inputs.values():
            if not isinstance(v, dict) or "selectors" not in v:
                continue
            for sel in (v.get("selectors") or []):
                if not isinstance(sel, dict):
                    continue
                if str(sel.get("from") or "").strip().upper() != "NEW":
                    continue
                dep_sid = str(sel.get("step_id") or "").strip()
                if dep_sid and dep_sid in by_id and dep_sid != sid:
                    deps[sid].add(dep_sid)

    ordered: List[str] = []
    temp: Set[str] = set()
    perm: Set[str] = set()

    def visit(n: str) -> None:
        if n in perm:
            return
        if n in temp:
            raise RuntimeError(f"Cycle detected in steps graph at step_id={n}")
        temp.add(n)
        for d in sorted(deps.get(n, set())):
            visit(d)
        temp.remove(n)
        perm.add(n)
        ordered.append(n)

    for sid in list(by_id.keys()):
        visit(sid)

    return [by_id[sid] for sid in ordered]


def _cache_key_for_step(module_id: str, tenant_id: str, resolved_inputs: Dict[str, Any], module_spec: Dict[str, Any]) -> Optional[str]:
    cache_cfg = module_spec.get("cache") or {}
    if not bool(cache_cfg.get("enabled", False)):
        return None
    keys = cache_cfg.get("key_inputs") or []
    if not isinstance(keys, list) or not keys:
        return None
    key_inputs = {k: resolved_inputs.get(k) for k in keys}
    from ..orchestration.module_exec import derive_cache_key
    return derive_cache_key(module_id=module_id, tenant_id=tenant_id, key_inputs=key_inputs)


def _cache_id_for_key(cache_key: str) -> str:
    return short_hash(sha256_bytes(cache_key.encode("utf-8")))


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    for p in src.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, target)


def run_orchestrator(repo_root: Path, billing_state_dir: Path, runtime_dir: Path, enable_github_releases: bool = False) -> None:
    reason_idx = _load_reason_index(repo_root)
    module_names = _load_module_display_names(repo_root)
    tenant_rel = _load_tenant_relationships(repo_root)
    deps_index = read_csv(repo_root / "maintenance-state" / "module_dependency_index.csv")
    prices = _load_module_prices(repo_root)
    artifacts_policy = _load_module_artifacts_policy(repo_root)

    billing = BillingState(billing_state_dir)
    billing_state_dir.mkdir(parents=True, exist_ok=True)

    # Orchestrator needs full state including mapping tables
    billing.validate_minimal(
        required_files=[
            "tenants_credits.csv",
            "transactions.csv",
            "transaction_items.csv",
            "promotion_redemptions.csv",
            "workorders_log.csv",
            "module_runs_log.csv",
            "github_releases_map.csv",
            "github_assets_map.csv",
        ]
    )

    tenants_credits = dedupe_tenants_credits(billing.load_table("tenants_credits.csv"))
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")
    workorders_log = billing.load_table("workorders_log.csv")
    module_runs_log = billing.load_table("module_runs_log.csv")
    # Note: GitHub Actions cache management is repo-scoped and handled via
    # platform/cache/cache_index.csv + cache-management workflow.
    promo_redemptions = billing.load_table("promotion_redemptions.csv")
    rel_map = billing.load_table("github_releases_map.csv")
    asset_map = billing.load_table("github_assets_map.csv")

    used_tx: Set[str] = {id_key(r.get("transaction_id")) for r in transactions if id_key(r.get("transaction_id"))}
    used_ti: Set[str] = {id_key(r.get("transaction_item_id")) for r in transaction_items if id_key(r.get("transaction_item_id"))}
    used_mr: Set[str] = {id_key(r.get("module_run_id")) for r in module_runs_log if id_key(r.get("module_run_id"))}
    used_rel: Set[str] = {id_key(r.get("release_id")) for r in rel_map if id_key(r.get("release_id"))}
    used_asset: Set[str] = {id_key(r.get("asset_id")) for r in asset_map if id_key(r.get("asset_id"))}

    runtime_dir.mkdir(parents=True, exist_ok=True)
    ensure_dir(runtime_dir)

    workorders = _discover_workorders(repo_root)
    for item in workorders:
        w = dict(item["workorder"])
        if not bool(w.get("enabled", True)):
            continue

        tenant_id = canon_tenant_id(item["tenant_id"])
        work_order_id = canon_work_order_id(item["work_order_id"])
        created_at = str((w.get("metadata") or {}).get("created_at") or utcnow_iso())
        started_at = utcnow_iso()

        # New workflow format (IFTTT-like chaining)
        if isinstance(w.get("steps"), list) and w.get("steps"):
            steps = _toposort_steps(list(w.get("steps") or []))
            # pricing (per step)
            step_cfgs: Dict[str, Dict[str, Any]] = {}
            step_items: Dict[str, List[Tuple[str, int]]] = {}
            step_module: Dict[str, str] = {}
            est_total = 0
            for s in steps:
                sid = str((s or {}).get("step_id") or "").strip()
                mid = canon_module_id((s or {}).get("module_id"))
                if not sid or not mid:
                    continue
                step_cfgs[sid] = s
                step_module[sid] = mid
                items = _module_line_items(prices, mid, bool((s or {}).get("purchase_release_artifacts", False)))
                step_items[sid] = items
                est_total += sum(amt for _, amt in items)

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
                workorders_log.append({
                    "work_order_id": work_order_id,
                    "tenant_id": tenant_id,
                    "status": "FAILED",
                    "created_at": created_at,
                    "started_at": started_at,
                    "ended_at": utcnow_iso(),
                    "note": "Insufficient credits",
                    "metadata_json": json.dumps({"workorder_path": item["path"], "workflow": "steps"}, separators=(",", ":")),
                })
                deny_tx = _new_id("transaction_id", used_tx)
                transactions.append({
                    "transaction_id": deny_tx,
                    "tenant_id": tenant_id,
                    "work_order_id": work_order_id,
                    "type": "DENIED",
                    "amount_credits": "0",
                    "created_at": utcnow_iso(),
                    "reason_code": rc,
                    "note": f"Insufficient credits: available={available}, required={est_total}",
                    "metadata_json": json.dumps({"workorder_path": item["path"], "available_credits": available, "required_credits": est_total, "workflow": "steps"}, separators=(",", ":")),
                })
                continue

            # spend transaction (debit)
            spend_tx = _new_id("transaction_id", used_tx)
            transactions.append({
                "transaction_id": spend_tx,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "SPEND",
                "amount_credits": str(-est_total),
                "created_at": utcnow_iso(),
                "reason_code": "",
                "note": f"Work order charge (steps={len(step_items)})",
                "metadata_json": json.dumps({"workorder_path": item["path"], "workflow": "steps"}, separators=(",", ":")),
            })

            # transaction items per step (for audit)
            per_step_cost: Dict[str, Dict[str, int]] = {}
            for sid, items in step_items.items():
                mid = step_module.get(sid, "")
                if not mid:
                    continue
                per_step_cost[sid] = {feat: amt for feat, amt in items}
                lbl = _module_label(mid, module_names)
                for feat, amt in items:
                    transaction_items.append({
                        "transaction_item_id": _new_id("transaction_item_id", used_ti),
                        "transaction_id": spend_tx,
                        "tenant_id": tenant_id,
                        "module_id": mid,
                        "feature": feat,
                        "type": "SPEND",
                        "amount_credits": str(-amt),
                        "created_at": utcnow_iso(),
                        "note": _note_for_spend_item(feat, lbl),
                        "metadata_json": json.dumps({"step_id": sid, "purchase_release_artifacts": bool(step_cfgs.get(sid, {}).get("purchase_release_artifacts", False)), "feature": feat}, separators=(",", ":")),
                    })

            # update balance
            trow["credits_available"] = str(available - est_total)
            trow["updated_at"] = utcnow_iso()

            mode = str(w.get("mode","" )).strip().upper() or "PARTIAL_ALLOWED"
            any_failed = False
            completed_steps: List[str] = []

            artifacts_by_step: Dict[str, Dict[str, Path]] = {}
            module_specs: Dict[str, Dict[str, Any]] = {}

            # Execute steps (DAG order)
            for s in steps:
                sid = str((s or {}).get("step_id") or "").strip()
                mid = canon_module_id((s or {}).get("module_id"))
                if not sid or not mid:
                    continue

                if mid not in module_specs:
                    module_specs[mid] = _load_module_spec(repo_root / "modules" / mid)
                in_ports, out_ports = _module_ports(module_specs.get(mid) or {})

                # Resolve inputs for this step
                step_inputs = (s or {}).get("inputs") or {}
                if not isinstance(step_inputs, dict):
                    step_inputs = {}
                resolved_inputs: Dict[str, Any] = {}
                missing_required: List[str] = []
                for pdef in in_ports:
                    pname = str((pdef or {}).get("name") or "").strip()
                    if not pname:
                        continue
                    required = bool((pdef or {}).get("required", False))
                    fmt = str((pdef or {}).get("format") or "text/plain")
                    card = str((pdef or {}).get("cardinality") or "single")
                    bind = step_inputs.get(pname)
                    val = _resolve_binding(
                        bind,
                        tenant_id=tenant_id,
                        repo_root=repo_root,
                        runtime_dir=runtime_dir,
                        artifacts_by_step=artifacts_by_step,
                        expected_format=fmt,
                        expected_cardinality=card,
                    )
                    if val is None and required:
                        missing_required.append(pname)
                    else:
                        resolved_inputs[pname] = val

                if missing_required:
                    # Record failure without executing module
                    mr_id = _new_id("module_run_id", used_mr)
                    m_started = utcnow_iso()
                    m_ended = utcnow_iso()
                    reason_code = _reason_code(reason_idx, "MODULE", mid, "missing_required_input") or _reason_code(reason_idx, "GLOBAL", "", "module_failed")
                    module_runs_log.append({
                        "module_run_id": mr_id,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "module_id": mid,
                        "status": "FAILED",
                        "created_at": created_at,
                        "started_at": m_started,
                        "ended_at": m_ended,
                        "reason_code": reason_code,
                        "report_path": "",
                        "output_ref": "",
                        "metadata_json": json.dumps({"workflow": "steps", "step_id": sid, "missing_inputs": missing_required}, separators=(",", ":")),
                    })
                    any_failed = True
                    if mode == "ALL_OR_NOTHING":
                        break
                    continue

                # Optional module-level cache reuse
                reuse_output_type = str((s or {}).get("reuse_output_type") or "").strip().lower()
                cache_key = _cache_key_for_step(mid, tenant_id, resolved_inputs, module_specs.get(mid) or {})
                cache_id = _cache_id_for_key(cache_key) if cache_key else ""
                cached_dir = runtime_dir / "cache" / cache_id if cache_id else None

                mr_id = _new_id("module_run_id", used_mr)
                m_started = utcnow_iso()

                module_path = repo_root / "modules" / mid
                out_dir = runtime_dir / "runs" / tenant_id / work_order_id / "steps" / sid / mid / mr_id
                ensure_dir(out_dir)

                if reuse_output_type == "cache" and cached_dir and cached_dir.exists():
                    _copy_tree(cached_dir, out_dir)
                    result = {"status": "COMPLETED", "files": []}
                else:
                    params = {
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "module_run_id": mr_id,
                        "step_id": sid,
                        "inputs": resolved_inputs,
                        "reuse_output_type": reuse_output_type,
                    }
                    try:
                        result = execute_module_runner(module_path=module_path, params=params, outputs_dir=out_dir)
                    except Exception as e:
                        result = {"status": "FAILED", "reason_slug": "exception", "message": str(e)}

                status = str(result.get("status","" )).strip().upper() or "FAILED"
                reason_slug = str(result.get("reason_slug","" )).strip() or str(result.get("reason_key","" )).strip()
                if status == "COMPLETED":
                    completed_steps.append(sid)
                    reason_code = ""
                    # write-through cache on success
                    if reuse_output_type == "cache" and cached_dir and cache_key:
                        ensure_dir(cached_dir)
                        _copy_tree(out_dir, cached_dir)
                else:
                    any_failed = True
                    if reason_slug:
                        reason_code = _reason_code(reason_idx, "MODULE", mid, reason_slug) or _reason_code(reason_idx, "GLOBAL", "", "module_failed")
                    else:
                        reason_code = _reason_code(reason_idx, "GLOBAL", "", "module_failed")

                report_path = str(result.get("report_path","" ) or "")
                output_ref = str(result.get("output_ref","" ) or "")

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
                    "metadata_json": json.dumps({"workflow": "steps", "step_id": sid, "inputs": resolved_inputs}, separators=(",", ":")),
                })

                # Register outputs for NEW bindings
                artifacts_by_step.setdefault(sid, {})
                for odef in out_ports:
                    oname = str((odef or {}).get("name") or "").strip()
                    fname = str((odef or {}).get("filename") or "").strip()
                    if oname and fname:
                        artifacts_by_step[sid][oname] = out_dir / fname

                # refund if configured refundable and step failed
                if status != "COMPLETED" and reason_code and reason_idx.refundable.get(reason_code, False):
                    items = step_items.get(sid, [])
                    refund_amt = sum(amt for _, amt in items)
                    refund_tx = _new_id("transaction_id", used_tx)
                    rdesc = reason_idx.description.get(reason_code, "")
                    transactions.append({
                        "transaction_id": refund_tx,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "type": "REFUND",
                        "amount_credits": str(refund_amt),
                        "created_at": utcnow_iso(),
                        "reason_code": reason_code,
                        "note": f"Refund for step_id={sid} module_id={mid}: {rdesc}",
                        "metadata_json": json.dumps({"module_run_id": mr_id, "step_id": sid, "workflow": "steps"}, separators=(",", ":")),
                    })
                    lbl = _module_label(mid, module_names)
                    for feat, amt in items:
                        transaction_items.append({
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "feature": feat,
                            "type": "REFUND",
                            "amount_credits": str(amt),
                            "created_at": utcnow_iso(),
                            "note": _note_for_refund_item(feat, lbl, reason_code, rdesc),
                            "metadata_json": json.dumps({"refund_for": mr_id, "feature": feat, "step_id": sid, "workflow": "steps"}, separators=(",", ":")),
                        })
                    # balance update
                    trow["credits_available"] = str(int(trow["credits_available"]) + refund_amt)
                    trow["updated_at"] = utcnow_iso()

                # publish artifacts to GitHub release (optional)
                purchase_release = bool((s or {}).get("purchase_release_artifacts", False))
                if enable_github_releases and purchase_release and artifacts_policy.get(mid, True) and status == "COMPLETED":
                    release_id = _new_id("github_release_asset_id", used_rel)
                    tag = f"r-{release_id}"
                    title = f"Artifacts {release_id}"
                    ensure_release(tag=tag, title=title, notes=f"tenant_id={tenant_id} work_order_id={work_order_id} step_id={sid} module_id={mid}")

                    staging = runtime_dir / "releases" / release_id
                    ensure_dir(staging)

                    items_list: List[Dict[str, Any]] = []
                    for fp in sorted(out_dir.rglob("*")):
                        if not fp.is_file():
                            continue
                        rel = fp.relative_to(out_dir).as_posix()
                        dst = staging / rel
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        dst.write_bytes(fp.read_bytes())
                        items_list.append({"name": rel, "sha256": sha256_file(dst), "size_bytes": str(dst.stat().st_size)})

                    manifest = {
                        "release_id": release_id,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "step_id": sid,
                        "module_id": mid,
                        "module_run_id": mr_id,
                        "created_at": utcnow_iso(),
                        "items": items_list,
                    }
                    manifest_path = staging / "manifest.json"
                    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

                    upload_release_assets(tag, [manifest_path, *[staging / i["name"] for i in items_list]], clobber=True)

                    gh_rel_id = str(get_release_numeric_id(tag))
                    rel_map.append({
                        "release_id": release_id,
                        "github_release_id": gh_rel_id,
                        "tag": tag,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "created_at": utcnow_iso(),
                    })

                    assets_ids = get_release_assets_numeric_ids(tag)
                    for asset_name, gh_asset_id in assets_ids.items():
                        asset_id = _new_id("github_release_asset_id", used_asset)
                        asset_map.append({
                            "asset_id": asset_id,
                            "github_asset_id": str(gh_asset_id),
                            "release_id": release_id,
                            "asset_name": asset_name,
                            "created_at": utcnow_iso(),
                        })

                    module_runs_log[-1]["output_ref"] = tag

                if status != "COMPLETED" and mode == "ALL_OR_NOTHING":
                    break

            ended_at = utcnow_iso()
            final_status = "COMPLETED"
            if any_failed and completed_steps:
                final_status = "PARTIAL"
            elif any_failed and not completed_steps:
                final_status = "FAILED"

            workorders_log.append({
                "work_order_id": work_order_id,
                "tenant_id": tenant_id,
                "status": final_status,
                "created_at": created_at,
                "started_at": started_at,
                "ended_at": ended_at,
                "note": "",
                "metadata_json": json.dumps({"workflow": "steps", "steps": [{"step_id": str((s or {}).get("step_id") or ""), "module_id": canon_module_id((s or {}).get("module_id"))} for s in steps]}, separators=(",", ":")),
            })
            continue

        # pricing
        requested_modules = [str(m.get("module_id","")).strip() for m in (w.get("modules") or []) if m.get("module_id")]
        ordered = _toposort_modules(requested_modules, deps_index)

        module_cfgs: Dict[str, Dict[str, Any]] = {}
        module_items: Dict[str, List[Tuple[str, int]]] = {}
        est_total = 0
        for m in (w.get("modules") or []):
            mid = canon_module_id(m.get("module_id",""))
            if not mid:
                continue
            module_cfgs[mid] = m
        for mid in ordered:
            cfg = module_cfgs.get(mid, {})
            items = _module_line_items(prices, mid, bool(cfg.get("purchase_release_artifacts", False)))
            module_items[mid] = items
            est_total += sum(i[1] for i in items)

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
            workorders_log.append({
                "work_order_id": work_order_id,
                "tenant_id": tenant_id,
                "status": "FAILED",
                "created_at": created_at,
                "started_at": started_at,
                "ended_at": utcnow_iso(),
                "note": "Insufficient credits",
                "metadata_json": json.dumps({"workorder_path": item["path"]}, separators=(",", ":")),
            })
            # record denied attempt in accounting SoT (no debit applied)
            deny_tx = _new_id("transaction_id", used_tx)
            transactions.append({
                "transaction_id": deny_tx,
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "type": "DENIED",
                "amount_credits": "0",
                "created_at": utcnow_iso(),
                "reason_code": rc,
                "note": f"Insufficient credits: available={available}, required={est_total}",
                "metadata_json": json.dumps({"workorder_path": item["path"], "available_credits": available, "required_credits": est_total}, separators=(",", ":")),
            })
            continue

        # spend transaction (debit)
        spend_tx = _new_id("transaction_id", used_tx)
        transactions.append({
            "transaction_id": spend_tx,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "type": "SPEND",
            "amount_credits": str(-est_total),
            "created_at": utcnow_iso(),
            "reason_code": "",
            "note": f"Work order charge (items={sum(len(v) for v in module_items.values())})",
            "metadata_json": json.dumps({"workorder_path": item["path"]}, separators=(",", ":")),
        })

        # transaction items per module (for audit)
        per_module_cost: Dict[str, Dict[str, int]] = {}
        for mid in ordered:
            cfg = module_cfgs.get(mid, {})
            items = module_items.get(mid, [])
            per_module_cost[mid] = {feat: amt for feat, amt in items}
            lbl = _module_label(mid, module_names)
            for feat, amt in items:
                transaction_items.append({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "feature": feat,
                    "type": "SPEND",
                    "amount_credits": str(-amt),
                    "created_at": utcnow_iso(),
                    "note": _note_for_spend_item(feat, lbl),
                    "metadata_json": json.dumps(
                        {
                            "purchase_release_artifacts": bool(cfg.get("purchase_release_artifacts", False)),
                            "feature": feat,
                        },
                        separators=(",", ":"),
                    ),
                })

        # update balance
        trow["credits_available"] = str(available - est_total)
        trow["updated_at"] = utcnow_iso()

        mode = str(w.get("mode","")).strip().upper() or "PARTIAL_ALLOWED"
        any_failed = False
        completed_modules: List[str] = []

        # Execute modules
        for mid in ordered:
            cfg = module_cfgs.get(mid, {})
            mr_id = _new_id("module_run_id", used_mr)
            m_started = utcnow_iso()

            # inputs: pass through
            params = {
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "module_run_id": mr_id,
                "inputs": cfg.get("inputs") or {},
                "reuse_output_type": str(cfg.get("reuse_output_type","")).strip(),
            }

            module_path = repo_root / "modules" / mid
            out_dir = runtime_dir / "runs" / tenant_id / work_order_id / mid / mr_id
            ensure_dir(out_dir)

            result = execute_module_runner(module_path=module_path, params=params, outputs_dir=out_dir)

            status = str(result.get("status","")).strip().upper() or "FAILED"
            reason_slug = str(result.get("reason_slug","")).strip() or str(result.get("reason_key","")).strip()
            if status == "COMPLETED":
                completed_modules.append(mid)
                reason_code = ""
            else:
                any_failed = True
                if reason_slug:
                    reason_code = _reason_code(reason_idx, "MODULE", mid, reason_slug) or _reason_code(reason_idx, "GLOBAL", "", "module_failed")
                else:
                    reason_code = _reason_code(reason_idx, "GLOBAL", "", "module_failed")

            # output ref / report path: optional
            report_path = str(result.get("report_path","") or "")
            output_ref = str(result.get("output_ref","") or "")

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
                "metadata_json": json.dumps({"outputs_dir": str(out_dir)}, separators=(",", ":")),
            })

            # refund if configured refundable and module failed
            if status != "COMPLETED" and reason_code and reason_idx.refundable.get(reason_code, False):
                costs = per_module_cost.get(mid, {})
                refund_amt = sum(int(v) for v in costs.values())
                if refund_amt > 0:
                    refund_tx = _new_id("transaction_id", used_tx)
                    lbl = _module_label(mid, module_names)
                    rdesc = reason_idx.description.get(reason_code, "")
                    reason_full = f"{reason_code} - {rdesc}" if rdesc else reason_code
                    transactions.append({
                        "transaction_id": refund_tx,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "type": "REFUND",
                        "amount_credits": str(refund_amt),
                        "created_at": utcnow_iso(),
                        "reason_code": reason_code,
                        "note": f"Refund due to failed module {lbl}: {reason_full}",
                        "metadata_json": json.dumps({"module_id": mid, "refund_for": mr_id}, separators=(",", ":")),
                    })
                    for feat, amt in sorted(costs.items()):
                        transaction_items.append({
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "feature": feat,
                            "type": "REFUND",
                            "amount_credits": str(int(amt)),
                            "created_at": utcnow_iso(),
                            "note": _note_for_refund_item(feat, lbl, reason_code, rdesc),
                            "metadata_json": json.dumps({"refund_for": mr_id, "feature": feat}, separators=(",", ":")),
                        })
                    # balance update
                    trow["credits_available"] = str(int(trow["credits_available"]) + refund_amt)
                    trow["updated_at"] = utcnow_iso()

            # publish artifacts to GitHub release (optional)
            purchase_release = bool(cfg.get("purchase_release_artifacts", False))
            if enable_github_releases and purchase_release and artifacts_policy.get(mid, True) and status == "COMPLETED":
                release_id = _new_id("github_release_asset_id", used_rel)
                tag = f"r-{release_id}"
                title = f"Artifacts {release_id}"
                ensure_release(tag=tag, title=title, notes=f"tenant_id={tenant_id} work_order_id={work_order_id} module_id={mid}")

                staging = runtime_dir / "releases" / release_id
                ensure_dir(staging)

                items: List[Dict[str, Any]] = []
                for fp in sorted(out_dir.rglob("*")):
                    if not fp.is_file():
                        continue
                    rel = fp.relative_to(out_dir).as_posix()
                    dst = staging / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_bytes(fp.read_bytes())
                    items.append({
                        "name": rel,
                        "sha256": sha256_file(dst),
                        "size_bytes": str(dst.stat().st_size),
                    })

                manifest = {
                    "release_id": release_id,
                    "tenant_id": tenant_id,
                    "work_order_id": work_order_id,
                    "module_id": mid,
                    "module_run_id": mr_id,
                    "created_at": utcnow_iso(),
                    "items": items,
                }
                manifest_path = staging / "manifest.json"
                manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

                upload_release_assets(tag, [manifest_path, *[staging / i["name"] for i in items]], clobber=True)

                # Map internal IDs -> GitHub numeric IDs
                gh_rel_id = str(get_release_numeric_id(tag))
                rel_map.append({
                    "release_id": release_id,
                    "github_release_id": gh_rel_id,
                    "tag": tag,
                    "tenant_id": tenant_id,
                    "work_order_id": work_order_id,
                    "created_at": utcnow_iso(),
                })

                assets_ids = get_release_assets_numeric_ids(tag)  # name -> numeric id
                # internal asset ids for each uploaded asset
                for asset_name, gh_asset_id in assets_ids.items():
                    asset_id = _new_id("github_release_asset_id", used_asset)
                    asset_map.append({
                        "asset_id": asset_id,
                        "github_asset_id": str(gh_asset_id),
                        "release_id": release_id,
                        "asset_name": asset_name,
                        "created_at": utcnow_iso(),
                    })

                # keep output_ref as tag
                module_runs_log[-1]["output_ref"] = tag

            if status != "COMPLETED" and mode == "ALL_OR_NOTHING":
                break

        ended_at = utcnow_iso()
        final_status = "COMPLETED"
        if any_failed and completed_modules:
            final_status = "PARTIAL"
        elif any_failed and not completed_modules:
            final_status = "FAILED"

        workorders_log.append({
            "work_order_id": work_order_id,
            "tenant_id": tenant_id,
            "status": final_status,
            "created_at": created_at,
            "started_at": started_at,
            "ended_at": ended_at,
            "note": "",
            "metadata_json": json.dumps({"requested_modules": ordered}, separators=(",", ":")),
        })

    # Persist billing state tables
    billing.save_table("tenants_credits.csv", tenants_credits, TENANTS_CREDITS_HEADERS)
    billing.save_table("transactions.csv", transactions, TRANSACTIONS_HEADERS)
    billing.save_table("transaction_items.csv", transaction_items, TRANSACTION_ITEMS_HEADERS)
    billing.save_table("promotion_redemptions.csv", promo_redemptions, PROMOTION_REDEMPTIONS_HEADERS)
    billing.save_table("workorders_log.csv", workorders_log, WORKORDERS_LOG_HEADERS)
    billing.save_table("module_runs_log.csv", module_runs_log, MODULE_RUNS_LOG_HEADERS)
    billing.save_table("github_releases_map.csv", rel_map, GITHUB_RELEASES_MAP_HEADERS)
    billing.save_table("github_assets_map.csv", asset_map, GITHUB_ASSETS_MAP_HEADERS)

    billing.write_state_manifest()
