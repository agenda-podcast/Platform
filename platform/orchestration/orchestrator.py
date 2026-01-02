from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..billing.state import BillingState
from ..github.releases import ensure_release, upload_release_assets
from ..utils.csvio import read_csv
from ..common.id_canonical import normalize_tenant_id, normalize_module_id
from ..utils.time import utcnow_iso
from ..utils.yamlio import read_yaml
from ..utils.ids import parse_reason_code
from .module_exec import ModuleExecResult, build_manifest_item, derive_cache_key, execute_module_runner
from .planner import load_dependency_index, topo_sort


@dataclass
class MaintenanceState:
    root: Path
    reason_by_key: Dict[Tuple[str, str, str], str]  # (scope, module_id, reason_key) -> reason_code
    policy_by_code: Dict[str, Dict[str, str]]
    artifacts_policy: Dict[str, bool]
    tenant_relationships: set[Tuple[str, str]]
    dependency_index: Dict[str, List[str]]


@dataclass
class OrchestratorConfig:
    repo_root: Path
    runtime_dir: Path
    billing_state_dir: Path
    enable_github_releases: bool = False



def load_maintenance_state(repo_root: Path) -> MaintenanceState:
    ms_root = repo_root / "maintenance-state"
    reason_catalog = read_csv(ms_root / "reason_catalog.csv")
    reason_by_key: Dict[Tuple[str, str, str], str] = {}
    for r in reason_catalog:
        code = str(r.get("reason_code", "")).strip()
        mod = normalize_module_id(r.get("module_id", ""))
        key = str(r.get("reason_key", "")).strip()
        g = str(r.get("g", "")).strip()
        if not (code and mod and key and g):
            continue
        scope = "GLOBAL" if g == "0" else "MODULE"
        reason_by_key[(scope, mod, key)] = code

    policy_rows = read_csv(ms_root / "reason_policy.csv")
    policy_by_code = {str(r.get("reason_code")): r for r in policy_rows if r.get("reason_code")}
    # artifacts policy: missing row => default allow
    ap_rows = read_csv(ms_root / "module_artifacts_policy.csv")
    artifacts_policy: Dict[str, bool] = {}
    for r in ap_rows:
        mid = normalize_module_id(r.get("module_id", ""))
        if not mid:
            continue
        artifacts_policy[mid] = str(r.get("platform_artifacts_enabled", "true")).lower() == "true"


    rel_rows = read_csv(ms_root / "tenant_relationships.csv")
    rel = set()
    for r in rel_rows:
        s = normalize_tenant_id(r.get("source_tenant_id", ""))
        t = normalize_tenant_id(r.get("target_tenant_id", ""))
        if s and t:
            rel.add((s, t))

    dep_index = load_dependency_index(ms_root / "module_dependency_index.csv")

    return MaintenanceState(
        root=ms_root,
        reason_by_key=reason_by_key,
        policy_by_code=policy_by_code,
        artifacts_policy=artifacts_policy,
        tenant_relationships=rel,
        dependency_index=dep_index,
    )


def _reason_code(ms: MaintenanceState, scope: str, module_id: str, reason_key: str) -> str:
    mid = normalize_module_id(module_id)
    code = ms.reason_by_key.get((scope, mid, reason_key))
    if code:
        return code
    # Fallback: if not found, raise to enforce catalog completeness.
    raise KeyError(f"Missing reason_code for ({scope}, {module_id}, {reason_key}). Run maintenance.")


def _is_refundable(ms: MaintenanceState, code: str) -> bool:
    p = ms.policy_by_code.get(code)
    if not p:
        return False
    return str(p.get("refundable", "false")).lower() == "true"

# -------------------------
# Work order discovery
# -------------------------

def discover_workorders(repo_root: Path) -> List[Dict[str, Any]]:
    """Return list of (tenant_id, workorder_path)."""
    tenants_dir = repo_root / "tenants"
    out: List[Dict[str, Any]] = []
    if not tenants_dir.exists():
        return out
    for tenant_dir in sorted(tenants_dir.iterdir()):
        if not tenant_dir.is_dir():
            continue
        tenant_yml = tenant_dir / "tenant.yml"
        if not tenant_yml.exists():
            continue
        tenant_id = normalize_tenant_id(tenant_dir.name)
        wdir = tenant_dir / "workorders"
        if not wdir.exists():
            continue
        for wo in sorted(wdir.glob("*.yml")):
            out.append({"tenant_id": tenant_id, "path": wo})
    return out


def load_workorder(tenant_id: str, path: Path) -> Optional[Dict[str, Any]]:
    y = read_yaml(path)
    if not y:
        return None
    if not bool(y.get("enabled", False)):
        return None
    y["tenant_id"] = tenant_id
    y["_path"] = str(path)
    return y


# -------------------------
# Billing helpers
# -------------------------

def _find_price(module_prices: List[Dict[str, str]], module_id: str) -> Tuple[int, int]:
    want = normalize_module_id(module_id)
    for r in module_prices:
        if normalize_module_id(r.get("module_id", "")) == want and str(r.get("active", "true")).lower() == "true":
            run = int(str(r.get("price_run_credits", "0")) or 0)
            rel = int(str(r.get("price_save_to_release_credits", "0")) or 0)
            return run, rel
    raise KeyError(f"Missing active module price for module {want}")

def _tenant_credit_row(tenants_credits: List[Dict[str, str]], tenant_id: str) -> Dict[str, str]:
    want = normalize_tenant_id(tenant_id)
    for r in tenants_credits:
        if normalize_tenant_id(r.get("tenant_id", "")) == want:
            return r
    raise KeyError(f"Tenant not found in tenants_credits.csv: {want}")

def _canonicalize_tenants_credits(tenants_credits: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Normalize tenant_id to canonical 10-digit form and deterministically merge duplicates.

    Duplicates can happen when tools coerce IDs to numbers (dropping leading zeros), producing both
    "0000000001" and "1" in billing-state. We canonicalize to 10 digits and keep the "best" row:
      1) most recent updated_at (ISO 8601 string compare is safe)
      2) higher credits_available
      3) later row in file

    We DO NOT sum balances across duplicates (avoids accidental double-counting).
    """
    best: Dict[str, Dict[str, str]] = {}
    best_meta: Dict[str, Tuple[str, int, int]] = {}  # (updated_at, credits, idx)

    for idx, r in enumerate(tenants_credits):
        tid = normalize_tenant_id(r.get("tenant_id", ""))
        if not tid:
            continue
        r["tenant_id"] = tid
        ts = str(r.get("updated_at", "")).strip()
        try:
            credits = int(str(r.get("credits_available", "0")).strip() or 0)
        except Exception:
            credits = 0

        if tid not in best:
            best[tid] = r
            best_meta[tid] = (ts, credits, idx)
            continue

        prev_ts, prev_credits, prev_idx = best_meta[tid]
        take = False
        if ts and prev_ts:
            if ts > prev_ts:
                take = True
            elif ts < prev_ts:
                take = False
            else:
                if credits > prev_credits:
                    take = True
                elif credits < prev_credits:
                    take = False
                else:
                    take = idx > prev_idx
        elif ts and not prev_ts:
            take = True
        elif not ts and prev_ts:
            take = False
        else:
            if credits > prev_credits:
                take = True
            elif credits < prev_credits:
                take = False
            else:
                take = idx > prev_idx

        if take:
            best[tid] = r
            best_meta[tid] = (ts, credits, idx)

    return [best[k] for k in sorted(best.keys())]

def _new_id(prefix: str) -> str:
    # stable-ish within a run: prefer GitHub run id if available.
    gh_run_id = os.environ.get("GITHUB_RUN_ID") or "local"
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{ts}-{gh_run_id}"

def _apply_promotions(
    promotions_catalog: List[Dict[str, str]],
    promotion_redemptions: List[Dict[str, str]],
    tenant_id: str,
    requested_promotions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return list of promo line items (negative amount).

    Promo definitions are repo-managed (platform/billing/promotions.csv).
    Redemption events (APPLIED/REFUNDED) are accounting state in billing-state.

    This scaffold applies promos conservatively:
    - exact code match
    - promo must be active
    - enforces max_uses_per_tenant if provided

    NOTE: date windows and rules_json are intentionally not enforced in this placeholder.
    """
    by_code = {str(p.get("code", "")).upper(): p for p in promotions_catalog}

    # Count net redemptions per tenant+promo_id (APPLIED minus REFUNDED)
    net_used = {}
    for e in promotion_redemptions:
        if normalize_tenant_id(e.get("tenant_id", "")) != normalize_tenant_id(tenant_id):
            continue
        pid = str(e.get("promo_id", "")).strip()
        if not pid:
            continue
        et = str(e.get("event_type", "")).strip().upper()
        delta = 0
        if et == "APPLIED":
            delta = 1
        elif et == "REFUNDED":
            delta = -1
        net_used[pid] = net_used.get(pid, 0) + delta

    out: List[Dict[str, Any]] = []
    for req in requested_promotions or []:
        code = str(req.get("code", "")).upper().strip()
        if not code:
            continue
        p = by_code.get(code)
        if not p:
            continue
        if str(p.get("active", "false")).lower() != "true":
            continue

        promo_id = str(p.get("promo_id", "")).strip()
        value = int(str(p.get("value_credits", "0")) or 0)
        if value <= 0 or not promo_id:
            continue

        max_uses_raw = str(p.get("max_uses_per_tenant", "")).strip()
        if max_uses_raw:
            try:
                max_uses = int(max_uses_raw)
            except Exception:
                max_uses = 0
            if max_uses > 0:
                if net_used.get(promo_id, 0) >= max_uses:
                    continue

        out.append({
            "code": code,
            "promo_id": promo_id,
            "type": str(p.get("type", "PROMO_CODE")),
            "amount_credits": -abs(value),
        })

    return out


def estimate_spend(
    module_prices: List[Dict[str, str]],
    workorder: Dict[str, Any],
    promotions_catalog: List[Dict[str, str]],
    promotion_redemptions: List[Dict[str, str]],
) -> Tuple[int, List[Dict[str, Any]]]:
    modules = workorder.get("modules", []) or []

    tenant_id = str(workorder.get("tenant_id", "")).strip()
    promo_items = _apply_promotions(promotions_catalog, promotion_redemptions, tenant_id, workorder.get("promotions", []) or [])

    total = 0
    line_items: List[Dict[str, Any]] = []

    for m in modules:
        mid = normalize_module_id(m.get("module_id", ""))
        run_price, rel_price = _find_price(module_prices, mid)
        total += run_price
        line_items.append({"name": f"module:{mid}", "category": "MODULE_RUN", "amount": run_price, "module_id": mid})
        if bool(m.get("purchase_release_artifacts", False)):
            total += rel_price
            line_items.append({"name": f"upload:{mid}", "category": "UPLOAD", "amount": rel_price, "module_id": mid})

    for p in promo_items:
        amt = int(p["amount_credits"])
        total += amt
        line_items.append({
            "name": f"promo:{p['code']}",
            "category": "PROMO",
            "amount": amt,
            "module_id": "",
            "promo_id": p["promo_id"],
            "promo_code": p["code"],
        })

    # Total spend may not be negative; clamp.
    total = max(0, total)
    return total, line_items

# -------------------------
# Billing mutations
# -------------------------

def _append_transaction(transactions: List[Dict[str, str]], tenant_id: str, work_order_id: str, tx_type: str, total_amount: int, metadata: Dict[str, Any]) -> str:
    tx_id = _new_id("tx")
    transactions.append({
        "transaction_id": tx_id,
        "tenant_id": tenant_id,
        "work_order_id": work_order_id,
        "type": tx_type,
        "total_amount_credits": str(total_amount),
        "created_at": utcnow_iso(),
        "metadata_json": json.dumps(metadata, sort_keys=True),
    })
    return tx_id


def _append_transaction_items(transaction_items: List[Dict[str, str]], tx_id: str, tenant_id: str, work_order_id: str, items: List[Dict[str, Any]], module_run_id_map: Dict[str, str]) -> None:
    for i, it in enumerate(items, start=1):
        name = str(it.get("name"))
        category = str(it.get("category"))
        amount = int(it.get("amount"))
        module_id = normalize_module_id(it.get("module_id", ""))
        module_run_id = module_run_id_map.get(module_id) if module_id else ""
        transaction_items.append({
            "transaction_item_id": f"ti-{tx_id}-{i:04d}",
            "transaction_id": tx_id,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "module_run_id": module_run_id or "",
            "name": name,
            "category": category,
            "amount_credits": str(amount),
            "reason_code": str(it.get("reason_code", "")) or "",
            "note": str(it.get("note", "")) or "",
        })

def _append_promotion_redemptions(
    promotion_redemptions: list[dict[str, str]],
    tenant_id: str,
    work_order_id: str,
    spend_items: list[dict[str, object]],
) -> None:
    """Append APPLIED promotion redemption events for promo spend items."""
    for it in spend_items:
        if str(it.get("category")) != "PROMO":
            continue
        promo_id = str(it.get("promo_id", "") or "").strip()
        promo_code = str(it.get("promo_code", "") or "").strip()
        if not promo_id:
            continue
        amount = int(it.get("amount", 0) or 0)
        promotion_redemptions.append({
            "event_id": _new_id("pr"),
            "tenant_id": tenant_id,
            "promo_id": promo_id,
            "work_order_id": work_order_id,
            "event_type": "APPLIED",
            "amount_credits": str(amount),
            "created_at": utcnow_iso(),
            "note": promo_code,
        })


# -------------------------
# Module execution
# -------------------------

def _parse_retention(ret: str) -> int:
    """Parse retention like 1d|1w|2m|1y into days."""
    s = (ret or "").strip().lower()
    if not s:
        return 7
    import re

    m = re.match(r"^(\d+)([dwmy])$", s)
    if not m:
        return 7
    n = int(m.group(1))
    u = m.group(2)
    if u == "d":
        return n
    if u == "w":
        return n * 7
    if u == "m":
        return n * 30
    if u == "y":
        return n * 365
    return 7


def _load_module_config(repo_root: Path, module_id: str) -> Dict[str, Any]:
    return read_yaml(repo_root / "modules" / module_id / "module.yml")


def _module_supports_artifacts(module_cfg: Dict[str, Any]) -> bool:
    return bool(module_cfg.get("supports_downloadable_artifacts", False))


def _platform_allows_artifacts(ms: MaintenanceState, module_id: str) -> bool:
    # Missing row => allow
    return ms.artifacts_policy.get(module_id, True)


def _cache_index_lookup(cache_index: List[Dict[str, str]], cache_key: str) -> Optional[Dict[str, str]]:
    for r in cache_index:
        if str(r.get("cache_key")) == cache_key:
            return r
    return None


def _cache_index_upsert(cache_index: List[Dict[str, str]], row: Dict[str, str]) -> None:
    existing = _cache_index_lookup(cache_index, row["cache_key"])
    if existing:
        existing.update(row)
    else:
        cache_index.append(row)


def _execute_module(
    cfg: OrchestratorConfig,
    ms: MaintenanceState,
    billing: BillingState,
    workorder: Dict[str, Any],
    module_spec: Dict[str, Any],
    module_run_id: str,
    outputs_dir: Path,
) -> ModuleExecResult:
    tenant_id = workorder["tenant_id"]
    work_order_id = workorder["work_order_id"]
    module_id = str(module_spec.get("module_id"))
    module_path = cfg.repo_root / "modules" / module_id

    module_cfg = _load_module_config(cfg.repo_root, module_id)

    # Artifact eligibility checks only apply when purchased.
    if bool(module_spec.get("purchase_release_artifacts", False)):
        if not _module_supports_artifacts(module_cfg) or not _platform_allows_artifacts(ms, module_id):
            try:
                rc = _reason_code(ms, "MODULE", module_id, "artifacts_not_eligible")
            except KeyError:
                rc = _reason_code(ms, "GLOBAL", "000000", "internal_error")
            return ModuleExecResult(status="FAILED", reason_code=rc)

    # Validate required params from module.yml
    params = module_spec.get("params", {}) or {}
    for inp in (module_cfg.get("inputs", []) or []):
        if not bool(inp.get("required", False)):
            continue
        name = str(inp.get("name"))
        if name not in params or params.get(name) in (None, ""):
            rc = _reason_code(ms, "MODULE", module_id, "missing_required_input")
            return ModuleExecResult(status="FAILED", reason_code=rc)

    # Cache key
    cache_cfg = module_cfg.get("cache", {}) or {}
    key_inputs = {k: params.get(k) for k in (cache_cfg.get("key_inputs") or [])}
    cache_key = derive_cache_key(module_id, tenant_id, key_inputs)

    reuse = str(module_spec.get("reuse_output_type", "new"))

    # Reuse types
    if reuse == "cache":
        cache_index = billing.load_table("cache_index.csv")
        hit = _cache_index_lookup(cache_index, cache_key)
        if hit:
            rc = _reason_code(ms, "MODULE", module_id, "skipped_cache")
            return ModuleExecResult(status="FAILED", reason_code=rc, cache_key=cache_key)

    # NOTE: release/assets reuse is intentionally minimal in the placeholder scaffold.
    if reuse == "release":
        tag = str(module_spec.get("release_tag", "")).strip()
        if not tag:
            rc = _reason_code(ms, "MODULE", module_id, "missing_required_input")
            return ModuleExecResult(status="FAILED", reason_code=rc)
        # Access control: source tenant is assumed encoded in tag or provided; for scaffold, deny cross-tenant.
        if (tenant_id, tenant_id) not in ms.tenant_relationships:
            rc = _reason_code(ms, "GLOBAL", "000000", "unauthorized_release_access")
            return ModuleExecResult(status="FAILED", reason_code=rc)

    if reuse == "assets":
        folder = str(module_spec.get("assets_folder_name", "")).strip()
        if not folder:
            rc = _reason_code(ms, "MODULE", module_id, "missing_required_input")
            return ModuleExecResult(status="FAILED", reason_code=rc)
        manifest_path = cfg.repo_root / "tenants" / tenant_id / "assets" / "outputs" / folder / "manifest.json"
        if not manifest_path.exists():
            rc = _reason_code(ms, "MODULE", module_id, "missing_required_input")
            return ModuleExecResult(status="FAILED", reason_code=rc)

    # Execute runner
    out = execute_module_runner(module_path, params, outputs_dir)

    # Cache persist if enabled and reuse==cache
    if reuse == "cache" and bool(cache_cfg.get("enabled", False)):
        from datetime import datetime, timezone, timedelta

        days = _parse_retention(str(module_spec.get("cache_retention_override") or cache_cfg.get("retention_default") or "1w"))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        exp = now + timedelta(days=days)
        cache_index = billing.load_table("cache_index.csv")
        _cache_index_upsert(cache_index, {
            "cache_key": cache_key,
            "tenant_id": tenant_id,
            "module_id": module_id,
            "created_at": now.isoformat().replace("+00:00", "Z"),
            "expires_at": exp.isoformat().replace("+00:00", "Z"),
            "cache_id": "",  # optional: can be populated by cache-prune workflow
        })
        billing.save_table("cache_index.csv", cache_index, ["cache_key", "tenant_id", "module_id", "created_at", "expires_at", "cache_id"])

    # Build manifest item (single file per placeholder)
    files = out.get("files") or []
    manifest_item = None
    if files:
        rel = str(files[0])
        fpath = outputs_dir / rel
        if fpath.exists():
            manifest_item = build_manifest_item(
                tenant_id=tenant_id,
                work_order_id=work_order_id,
                module_id=module_id,
                item_id="001",
                file_path=fpath,
                mime_type="text/plain",
            )

    return ModuleExecResult(status="COMPLETED", cache_key=cache_key, manifest_item=manifest_item)

# -------------------------
# Orchestration
# -------------------------

def _write_workorder_log(
    workorders_log: List[Dict[str, str]],
    workorder: Dict[str, Any],
    status: str,
    reason_code: str,
    started_at: str,
    finished_at: str,
    requested_modules: List[str],
) -> None:
    workorders_log.append({
        "work_order_id": str(workorder["work_order_id"]),
        "tenant_id": str(workorder["tenant_id"]),
        "status": status,
        "reason_code": reason_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "workorder_mode": str(workorder.get("mode", "")),
        "requested_modules": json.dumps(requested_modules),
        "metadata_json": json.dumps(workorder.get("metadata", {}), sort_keys=True),
    })


def _append_module_run_log(
    module_runs_log: List[Dict[str, str]],
    module_run_id: str,
    workorder: Dict[str, Any],
    module_id: str,
    result: ModuleExecResult,
    started_at: str,
    finished_at: str,
    reuse_output_type: str,
    reuse_reference: str,
    published_release_tag: str,
    release_manifest_name: str,
) -> None:
    module_runs_log.append({
        "module_run_id": module_run_id,
        "work_order_id": str(workorder["work_order_id"]),
        "tenant_id": str(workorder["tenant_id"]),
        "module_id": module_id,
        "status": result.status,
        "reason_code": result.reason_code or "",
        "started_at": started_at,
        "finished_at": finished_at,
        "reuse_output_type": reuse_output_type,
        "reuse_reference": reuse_reference,
        "cache_key_used": result.cache_key or "",
        "published_release_tag": published_release_tag,
        "release_manifest_name": release_manifest_name,
        "metadata_json": json.dumps({}, sort_keys=True),
    })


def _publish_release_if_needed(cfg: OrchestratorConfig, workorder: Dict[str, Any], module_results: List[Tuple[str, ModuleExecResult, Dict[str, Any]]]) -> Tuple[str, str]:
    """Publish purchased artifacts to a release.

    Returns (release_tag, manifest_name) or ("", "") if nothing to publish.
    """
    if not cfg.enable_github_releases:
        return "", ""

    tenant_id = workorder["tenant_id"]
    work_order_id = workorder["work_order_id"]

    items: List[Dict[str, Any]] = []
    staging = cfg.runtime_dir / "releases" / f"{tenant_id}-{work_order_id}"
    staging.mkdir(parents=True, exist_ok=True)

    for module_id, res, module_spec in module_results:
        if res.status != "COMPLETED":
            continue
        if not bool(module_spec.get("purchase_release_artifacts", False)):
            continue
        if not res.manifest_item:
            continue
        src = Path(res.manifest_item["_source_path"])
        dst = staging / res.manifest_item["filename"]
        dst.write_bytes(src.read_bytes())
        item = {k: v for k, v in res.manifest_item.items() if not k.startswith("_")}
        items.append(item)

    if not items:
        return "", ""

    manifest = {
        "owning_tenant_id": tenant_id,
        "work_order_id": work_order_id,
        "published_at": utcnow_iso(),
        "items": items,
    }
    manifest_name = f"{tenant_id}-{work_order_id}-manifest.json"
    manifest_path = staging / manifest_name
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    tag = f"release-{tenant_id}-{work_order_id}"
    ensure_release(tag, title=f"{tenant_id} {work_order_id}")
    upload_release_assets(tag, [manifest_path, *[staging / i["filename"] for i in items]], clobber=True)

    return tag, manifest_name


def orchestrate_workorder(cfg: OrchestratorConfig, ms: MaintenanceState, billing: BillingState, workorder: Dict[str, Any]) -> None:
    tenant_id = normalize_tenant_id(workorder["tenant_id"])
    work_order_id = str(workorder.get("work_order_id"))

    started_at = utcnow_iso()
    # Load billing tables (accounting SoT)
    tenants_credits = billing.load_table("tenants_credits.csv")
    tenants_credits = _canonicalize_tenants_credits(tenants_credits)
    # Persist canonical form early so a failed run still repairs state
    billing.save_table("tenants_credits.csv", tenants_credits, ["tenant_id","credits_available","updated_at","status"])
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")
    workorders_log = billing.load_table("workorders_log.csv")
    module_runs_log = billing.load_table("module_runs_log.csv")
    promotion_redemptions = billing.load_table("promotion_redemptions.csv")
    # Load billing configuration from the repository (admin-managed)
    billing_cfg = cfg.repo_root / "platform" / "billing"
    module_prices = read_csv(billing_cfg / "module_prices.csv")
    promotions_catalog = read_csv(billing_cfg / "promotions.csv")

    # Tenant status check
    trow = _tenant_credit_row(tenants_credits, tenant_id)
    if str(trow.get("status", "active")).lower() != "active":
        rc = _reason_code(ms, "GLOBAL", "000000", "tenant_suspended")
        _write_workorder_log(workorders_log, workorder, "FAILED", rc, started_at, utcnow_iso(), [])
        billing.save_table("workorders_log.csv", workorders_log, list(workorders_log[0].keys()) if workorders_log else [
            "work_order_id","tenant_id","status","reason_code","started_at","finished_at","github_run_id","workorder_mode","requested_modules","metadata_json"
        ])
        return

    # Dependency planning
    requested_specs = workorder.get("modules", []) or []
    requested_module_ids = [str(m.get("module_id")) for m in requested_specs]
    try:
        ordered = topo_sort(requested_module_ids, ms.dependency_index)
    except Exception:
        rc = _reason_code(ms, "GLOBAL", "000000", "workorder_invalid")
        _write_workorder_log(workorders_log, workorder, "FAILED", rc, started_at, utcnow_iso(), requested_module_ids)
        billing.save_table("workorders_log.csv", workorders_log, list(workorders_log[0].keys()) if workorders_log else [
            "work_order_id","tenant_id","status","reason_code","started_at","finished_at","github_run_id","workorder_mode","requested_modules","metadata_json"
        ])
        return

    # Estimate + credit check
    est_total, spend_items = estimate_spend(module_prices, workorder, promotions_catalog, promotion_redemptions)
    available = int(str(trow.get("credits_available", "0")) or 0)
    if available < est_total:
        rc = _reason_code(ms, "GLOBAL", "000000", "not_enough_credits")
        _write_workorder_log(workorders_log, workorder, "FAILED", rc, started_at, utcnow_iso(), ordered)
        billing.save_table("workorders_log.csv", workorders_log, list(workorders_log[0].keys()) if workorders_log else [
            "work_order_id","tenant_id","status","reason_code","started_at","finished_at","github_run_id","workorder_mode","requested_modules","metadata_json"
        ])
        return

    # Spend transaction (debit)
    module_run_id_map: Dict[str, str] = {}
    for m in ordered:
        module_run_id_map[m] = _new_id(f"mr-{tenant_id}-{work_order_id}-{m}")

    spend_tx_id = _append_transaction(transactions, tenant_id, work_order_id, "SPEND", est_total, {"workorder_path": workorder.get("_path", "")})

    tx_items = []
    for it in spend_items:
        item = {"name": it.get("name"), "category": it.get("category"), "amount": it.get("amount"), "module_id": it.get("module_id", ""), "reason_code": "", "note": ""}
        if str(it.get("category")) == "PROMO":
            item["note"] = str(it.get("promo_code", ""))
            item["promo_id"] = it.get("promo_id")
            item["promo_code"] = it.get("promo_code")
        tx_items.append(item)

    _append_transaction_items(transaction_items, spend_tx_id, tenant_id, work_order_id, tx_items, module_run_id_map)

    # Record promo redemption events (accounting state)
    _append_promotion_redemptions(promotion_redemptions, tenant_id, work_order_id, spend_items)

    # Update credits
    trow["credits_available"] = str(available - est_total)
    trow["updated_at"] = utcnow_iso()

    # Execute modules
    module_results: List[Tuple[str, ModuleExecResult, Dict[str, Any]]] = []
    outputs_root = cfg.runtime_dir / "workorders" / tenant_id / work_order_id

    spec_by_id = {str(s.get("module_id")): s for s in requested_specs}

    for mid in ordered:
        spec = spec_by_id[mid]
        mr_id = module_run_id_map[mid]
        m_started = utcnow_iso()
        out_dir = outputs_root / f"module-{mid}"
        out_dir.mkdir(parents=True, exist_ok=True)

        res = _execute_module(cfg, ms, billing, workorder, spec, mr_id, out_dir)
        m_finished = utcnow_iso()
        module_results.append((mid, res, spec))

        _append_module_run_log(
            module_runs_log,
            module_run_id=mr_id,
            workorder=workorder,
            module_id=mid,
            result=res,
            started_at=m_started,
            finished_at=m_finished,
            reuse_output_type=str(spec.get("reuse_output_type", "new")),
            reuse_reference=str(spec.get("reuse_reference", "")),
            published_release_tag="",
            release_manifest_name="",
        )

        # STRICT mode: stop on first failure
        if res.status != "COMPLETED" and str(workorder.get("mode", "STRICT")).upper() == "STRICT":
            break

    # Publish (optional)
    tag, manifest_name = _publish_release_if_needed(cfg, workorder, module_results)
    if tag:
        # Update module run logs with publication details
        for r in module_runs_log:
            if r.get("work_order_id") == work_order_id and r.get("tenant_id") == tenant_id:
                if str(r.get("status")) == "COMPLETED":
                    r["published_release_tag"] = tag
                    r["release_manifest_name"] = manifest_name

    # Refund calculation
    # Build gross_failed + deals_total
    deals_total = sum(abs(int(it["amount"])) for it in spend_items if it["category"] == "PROMO")
    refundable_failed: List[Tuple[str, int, str]] = []  # (module_id, gross, reason_code)
    for mid, res, spec in module_results:
        if res.status == "COMPLETED":
            continue
        if not res.reason_code:
            continue
        if not _is_refundable(ms, res.reason_code):
            continue
        gross = 0
        for it in spend_items:
            if it.get("module_id") == mid and it["category"] in ("MODULE_RUN", "UPLOAD"):
                gross += int(it["amount"])
        refundable_failed.append((mid, gross, res.reason_code))

    # Allocate deals against refunds in order
    remaining_deals = deals_total
    refund_items: List[Dict[str, Any]] = []
    refund_total = 0
    for mid, gross, rc in refundable_failed:
        net = gross
        if remaining_deals > 0:
            applied = min(net, remaining_deals)
            net -= applied
            remaining_deals -= applied
        if net <= 0:
            continue
        refund_total += net
        refund_items.append({"name": f"refund:{mid}", "category": "REFUND", "amount": -net, "module_id": mid, "reason_code": rc, "note": "auto"})

    if refund_total > 0:
        refund_tx_id = _append_transaction(transactions, tenant_id, work_order_id, "REFUND", -refund_total, {"method": "auto"})
        _append_transaction_items(transaction_items, refund_tx_id, tenant_id, work_order_id,
                                 [{"name": it["name"], "category": it["category"], "amount": int(it["amount"]), "module_id": it.get("module_id",""), "reason_code": it.get("reason_code",""), "note": it.get("note",""),} for it in refund_items],
                                 module_run_id_map)
        # credit back
        trow["credits_available"] = str(int(trow["credits_available"]) + refund_total)
        trow["updated_at"] = utcnow_iso()

    # Workorder status
    completed = [m for m, res, _ in module_results if res.status == "COMPLETED"]
    failed = [m for m, res, _ in module_results if res.status != "COMPLETED"]
    if not failed and len(completed) == len(ordered):
        wo_status = "COMPLETED"
        wo_rc = ""
    elif completed and str(workorder.get("mode", "STRICT")).upper() == "PARTIAL_ALLOWED":
        wo_status = "PARTIALLY_COMPLETED"
        wo_rc = ""
    else:
        wo_status = "FAILED"
        wo_rc = module_results[-1][1].reason_code or _reason_code(ms, "GLOBAL", "000000", "internal_error")

    finished_at = utcnow_iso()
    _write_workorder_log(workorders_log, workorder, wo_status, wo_rc, started_at, finished_at, ordered)

    # Persist billing tables
    billing.save_table("transactions.csv", transactions, ["transaction_id","tenant_id","work_order_id","type","total_amount_credits","created_at","metadata_json"])
    billing.save_table("transaction_items.csv", transaction_items, ["transaction_item_id","transaction_id","tenant_id","work_order_id","module_run_id","name","category","amount_credits","reason_code","note"])
    billing.save_table("tenants_credits.csv", tenants_credits, ["tenant_id","credits_available","updated_at","status"])
    billing.save_table("promotion_redemptions.csv", promotion_redemptions, ["event_id","tenant_id","promo_id","work_order_id","event_type","amount_credits","created_at","note"])
    billing.save_table("workorders_log.csv", workorders_log, ["work_order_id","tenant_id","status","reason_code","started_at","finished_at","github_run_id","workorder_mode","requested_modules","metadata_json"])
    billing.save_table("module_runs_log.csv", module_runs_log, ["module_run_id","work_order_id","tenant_id","module_id","status","reason_code","started_at","finished_at","reuse_output_type","reuse_reference","cache_key_used","published_release_tag","release_manifest_name","metadata_json"])


def run_orchestrator(repo_root: Path, billing_state_dir: Path, runtime_dir: Path, enable_github_releases: bool = False) -> None:
    cfg = OrchestratorConfig(
        repo_root=repo_root,
        runtime_dir=runtime_dir,
        billing_state_dir=billing_state_dir,
        enable_github_releases=enable_github_releases,
    )
    ms = load_maintenance_state(repo_root)
    billing = BillingState(billing_state_dir)
    billing.validate_minimal()

    # Execute all enabled work orders
    for item in discover_workorders(repo_root):
        wo = load_workorder(item["tenant_id"], Path(item["path"]))
        if not wo:
            continue
        orchestrate_workorder(cfg, ms, billing, wo)

    # Update state manifest
    billing.write_state_manifest()
