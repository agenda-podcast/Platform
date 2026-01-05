from __future__ import annotations

import json
import os
import hashlib
import shutil
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
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


def _parse_iso_z(s: str) -> datetime:
    if not s:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


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
    label_by_code: Dict[str, str]            # reason_code -> human label


def _humanize_slug(slug: str) -> str:
    s = (slug or "").strip().replace("_", " ")
    return s[:1].upper() + s[1:] if s else ""


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
    label_by_code: Dict[str, str] = {}
    for r in catalog:
        scope = str(r.get("scope", "")).strip().upper()
        module_id = str(r.get("module_id", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        code = str(r.get("reason_code", "")).strip()
        desc = str(r.get("description", "")).strip()
        if not (scope and slug and code):
            continue
        if scope == "GLOBAL":
            module_id = ""
        by_key[(scope, module_id, slug)] = code

        # Human labels for notes/logging. Prefer a short, readable slug; fallback to description.
        if code not in label_by_code:
            label_by_code[code] = _humanize_slug(slug) or desc or code

    refundable: Dict[str, bool] = {}
    for r in policy:
        code = str(r.get("reason_code", "")).strip()
        if not code:
            continue
        refundable[code] = str(r.get("refundable", "")).strip().lower() == "true"

    return ReasonIndex(by_key=by_key, refundable=refundable, label_by_code=label_by_code)


def _reason_label(idx: ReasonIndex, reason_code: str) -> str:
    rc = (reason_code or "").strip()
    if not rc:
        return ""
    return idx.label_by_code.get(rc, rc)


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


def _price_parts_for_module(prices: Dict[str, Dict[str, str]], module_id: str, purchase_release_artifacts: bool) -> Tuple[int, int]:
    """Return (run_price, release_artifacts_price) in credits."""
    row = prices.get(module_id)
    if not row:
        return (0, 0)
    try:
        run_price = int(str(row.get("price_run_credits", "0")).strip() or "0")
        rel_price = int(str(row.get("price_save_to_release_credits", "0")).strip() or "0")
    except Exception:
        return (0, 0)
    return (run_price, rel_price if purchase_release_artifacts else 0)


def _price_for_module(prices: Dict[str, Dict[str, str]], module_id: str, purchase_release_artifacts: bool) -> int:
    run_p, rel_p = _price_parts_for_module(prices, module_id, purchase_release_artifacts)
    return run_p + rel_p


def _load_module_display_names(repo_root: Path) -> Dict[str, str]:
    """Load optional human-readable module names from modules/*/module.yml (key: module_id)."""
    out: Dict[str, str] = {}
    modules_dir = repo_root / "modules"
    if not modules_dir.exists():
        return out
    for d in modules_dir.iterdir():
        if not d.is_dir():
            continue
        yml = d / "module.yml"
        if not yml.exists():
            continue
        try:
            data = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        mid = canon_module_id(data.get("module_id", "") or d.name)
        if not mid:
            continue
        name = str(data.get("name", "") or "").strip()
        if name:
            out[mid] = name
    return out


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


def run_orchestrator(repo_root: Path, billing_state_dir: Path, runtime_dir: Path, enable_github_releases: bool = False) -> None:
    run_since = utcnow_iso()
    reason_idx = _load_reason_index(repo_root)
    tenant_rel = _load_tenant_relationships(repo_root)
    deps_index = read_csv(repo_root / "maintenance-state" / "module_dependency_index.csv")
    prices = _load_module_prices(repo_root)
    artifacts_policy = _load_module_artifacts_policy(repo_root)
    module_names = _load_module_display_names(repo_root)

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
    cache_index = billing.load_table("cache_index.csv")
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

    # Local module output cache (persisted across workflow runs via actions/cache).
    cache_root = runtime_dir / "cache_outputs"
    ensure_dir(cache_root)

    workorders = _discover_workorders(repo_root)

    # Run-scoped summary (useful in CI logs)
    print("\nORCHESTRATOR RUN-SCOPED SUMMARY")
    print(f"billing_state_dir: {billing_state_dir}")
    print(f"runtime_dir:       {runtime_dir}")
    print(f"tenants_dir:       {repo_root / 'tenants'}")
    print(f"since:             {run_since}")
    print("")
    print(f"Discovered workorders: {len(workorders)}")
    for it in workorders:
        print(f" - {it.get('path')}")
    print("")


    # Auto-enable GitHub Releases when artifacts were purchased.
    # This keeps offline/local runs stable while ensuring "download artifacts" works in GitHub Actions.
    if not enable_github_releases:
        token_present = bool(os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"))
        if token_present:
            wants_releases = False
            for it in workorders:
                w = dict(it.get("workorder") or {})
                if not bool(w.get("enabled", True)):
                    continue
                for m in (w.get("modules") or []):
                    mid = canon_module_id(m.get("module_id", ""))
                    if not mid:
                        continue
                    if bool(m.get("purchase_release_artifacts", False)) and artifacts_policy.get(mid, True):
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
        created_at = str((w.get("metadata") or {}).get("created_at") or utcnow_iso())
        started_at = utcnow_iso()

        # pricing
        requested_modules = [str(m.get("module_id","")).strip() for m in (w.get("modules") or []) if m.get("module_id")]
        ordered = _toposort_modules(requested_modules, deps_index)

        print(f"[orchestrator] work_order_id={work_order_id} tenant_id={tenant_id} modules={ordered}")

        module_cfgs: Dict[str, Dict[str, Any]] = {}
        est_total = 0
        for m in (w.get("modules") or []):
            mid = canon_module_id(m.get("module_id",""))
            if not mid:
                continue
            module_cfgs[mid] = m
        for mid in ordered:
            cfg = module_cfgs.get(mid, {})
            est_total += _price_for_module(prices, mid, bool(cfg.get("purchase_release_artifacts", False)))

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

        # spend transaction (debit)
        spend_tx = _new_id("transaction_id", used_tx)
        # Human-readable module list for notes. Prefer module name; never append raw IDs when a name exists.
        modules_human = ", ".join([module_names.get(m) or m for m in ordered])
        transactions.append({
            "transaction_id": spend_tx,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "type": "SPEND",
            "amount_credits": str(-est_total),
            "created_at": utcnow_iso(),
            "reason_code": "",
            "note": f"Work order spend: {modules_human}",
            "metadata_json": json.dumps({"workorder_path": item["path"], "modules": ordered}, separators=(",", ":")),
        })

        # transaction items per module (for audit)
        # Store both total cost and itemized parts so refunds can be itemized too.
        per_module_cost: Dict[str, int] = {}
        per_module_parts: Dict[str, tuple[int, int]] = {}
        for mid in ordered:
            cfg = module_cfgs.get(mid, {})
            run_p, rel_p = _price_parts_for_module(prices, mid, bool(cfg.get("purchase_release_artifacts", False)))
            cost = run_p + rel_p
            per_module_cost[mid] = cost
            per_module_parts[mid] = (run_p, rel_p)
            m_label = module_names.get(mid) or mid
            transaction_items.append({
                "transaction_item_id": _new_id("transaction_item_id", used_ti),
                "transaction_id": spend_tx,
                "tenant_id": tenant_id,
                "module_id": mid,
                "feature": "RUN",
                "type": "SPEND",
                "amount_credits": str(-run_p),
                "created_at": utcnow_iso(),
                "note": f"Module run spend: {m_label}",
                "metadata_json": json.dumps({"purchase_release_artifacts": bool(cfg.get("purchase_release_artifacts", False))}, separators=(",", ":")),
            })

            # If saving artifacts to Releases was purchased, itemize it separately.
            if rel_p > 0:
                transaction_items.append({
                    "transaction_item_id": _new_id("transaction_item_id", used_ti),
                    "transaction_id": spend_tx,
                    "tenant_id": tenant_id,
                    "module_id": mid,
                    "feature": "RELEASE_ARTIFACTS",
                    "type": "SPEND",
                    "amount_credits": str(-rel_p),
                    "created_at": utcnow_iso(),
                    "note": f"Artifacts-to-Release spend: {m_label}",
                    "metadata_json": json.dumps({"purchase_release_artifacts": True}, separators=(",", ":")),
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

            # ------------------------------------------------------------------
            # Performance cache: reuse module outputs from runtime/cache_outputs
            # when reuse_output_type == "cache".
            # ------------------------------------------------------------------
            reuse_type = str(cfg.get("reuse_output_type", "")).strip().lower()
            key_inputs = cfg.get("inputs") or {}
            cache_key = derive_cache_key(module_id=mid, tenant_id=tenant_id, key_inputs=key_inputs)
            cache_dir = cache_root / _cache_dirname(cache_key)

            cache_row = None
            for r in cache_index:
                if str(r.get("cache_key", "")).strip() == cache_key:
                    cache_row = r
                    break

            cache_valid = False
            if cache_row is not None:
                try:
                    exp = _parse_iso_z(str(cache_row.get("expires_at", "")))
                    cache_valid = exp > datetime.now(timezone.utc)
                except Exception:
                    cache_valid = False

            if reuse_type == "cache" and _dir_has_files(cache_dir) and (cache_row is None or cache_valid):
                _copy_tree(cache_dir, out_dir)
                result = {
                    "status": "COMPLETED",
                    "reason_slug": "",
                    "report_path": "",
                    "output_ref": f"cache:{cache_key}",
                    "_cache_hit": True,
                }
            else:
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
                "metadata_json": json.dumps({"outputs_dir": str(out_dir), "cache_key": cache_key, "cache_hit": cache_hit}, separators=(",", ":")),
            })

            # Persist successful outputs into the local module cache.
            # Cache is only reused when reuse_output_type == "cache".
            if status == "COMPLETED":
                if not cache_hit:
                    _copy_tree(out_dir, cache_dir)

                now_dt = datetime.now(timezone.utc).replace(microsecond=0)
                exp_dt = now_dt + timedelta(days=30)
                if cache_row is None:
                    cache_index.append({
                        "cache_key": cache_key,
                        "tenant_id": tenant_id,
                        "module_id": mid,
                        "created_at": now_dt.isoformat().replace("+00:00", "Z"),
                        "expires_at": exp_dt.isoformat().replace("+00:00", "Z"),
                        "cache_id": "",
                    })
                else:
                    # Extend expiry forward if needed; keep created_at stable.
                    try:
                        old_exp = _parse_iso_z(str(cache_row.get("expires_at", "")))
                    except Exception:
                        old_exp = datetime(1970, 1, 1, tzinfo=timezone.utc)
                    if exp_dt > old_exp:
                        cache_row["expires_at"] = exp_dt.isoformat().replace("+00:00", "Z")
                    cache_row["tenant_id"] = tenant_id
                    cache_row["module_id"] = mid

            # refund if configured refundable and module failed
            # IMPORTANT: refunds must be itemized to mirror spend line-items (RUN / RELEASE_ARTIFACTS).
            if status != "COMPLETED" and reason_code and reason_idx.refundable.get(reason_code, False):
                # Prefer the itemized parts captured at spend time. If missing for any reason,
                # recompute from pricing so refunds are always recorded and itemized.
                parts = per_module_parts.get(mid)
                if parts is None:
                    run_p, rel_p = _price_parts_for_module(prices, mid, bool(cfg.get("purchase_release_artifacts", False)))
                else:
                    run_p, rel_p = parts
                refund_amt = int(run_p) + int(rel_p)
                if refund_amt > 0:
                    refund_tx = _new_id("transaction_id", used_tx)
                    m_label = module_names.get(mid) or mid
                    transactions.append({
                        "transaction_id": refund_tx,
                        "tenant_id": tenant_id,
                        "work_order_id": work_order_id,
                        "type": "REFUND",
                        "amount_credits": str(refund_amt),
                        "created_at": utcnow_iso(),
                        "reason_code": reason_code,
                        "note": f"Refund: {m_label} (reason={_reason_label(reason_idx, reason_code)})",
                        "metadata_json": json.dumps({"module_id": mid, "refund_for": mr_id, "spend_transaction_id": spend_tx}, separators=(",", ":")),
                    })

                    # RUN refund item
                    if int(run_p) > 0:
                        transaction_items.append({
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "feature": "RUN",
                            "type": "REFUND",
                            "amount_credits": str(int(run_p)),
                            "created_at": utcnow_iso(),
                            "note": f"Refund item (RUN): {m_label} (reason={_reason_label(reason_idx, reason_code)})",
                            "metadata_json": json.dumps({"refund_for": mr_id, "feature": "RUN", "spend_transaction_id": spend_tx}, separators=(",", ":")),
                        })

                    # RELEASE_ARTIFACTS refund item (only if it was purchased as part of pricing)
                    if int(rel_p) > 0:
                        transaction_items.append({
                            "transaction_item_id": _new_id("transaction_item_id", used_ti),
                            "transaction_id": refund_tx,
                            "tenant_id": tenant_id,
                            "module_id": mid,
                            "feature": "RELEASE_ARTIFACTS",
                            "type": "REFUND",
                            "amount_credits": str(int(rel_p)),
                            "created_at": utcnow_iso(),
                            "note": f"Refund item (RELEASE_ARTIFACTS): {m_label} (reason={_reason_label(reason_idx, reason_code)})",
                            "metadata_json": json.dumps({"refund_for": mr_id, "feature": "RELEASE_ARTIFACTS", "spend_transaction_id": spend_tx}, separators=(",", ":")),
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

        print(f"[orchestrator] work_order_id={work_order_id} status={final_status} completed_modules={completed_modules}")

        workorders_log.append({
            "work_order_id": work_order_id,
            "tenant_id": tenant_id,
            "status": final_status,
            "created_at": created_at,
            "started_at": started_at,
            "ended_at": ended_at,
            "note": f"{final_status}: {modules_human}",
            "metadata_json": json.dumps({"requested_modules": ordered, "completed_modules": completed_modules, "any_failed": any_failed}, separators=(",", ":")),
        })

    # Persist billing state tables
    billing.save_table("tenants_credits.csv", tenants_credits, TENANTS_CREDITS_HEADERS)
    billing.save_table("transactions.csv", transactions, TRANSACTIONS_HEADERS)
    billing.save_table("transaction_items.csv", transaction_items, TRANSACTION_ITEMS_HEADERS)
    billing.save_table("promotion_redemptions.csv", promo_redemptions, PROMOTION_REDEMPTIONS_HEADERS)
    billing.save_table("cache_index.csv", cache_index, CACHE_INDEX_HEADERS)
    billing.save_table("workorders_log.csv", workorders_log, WORKORDERS_LOG_HEADERS)
    billing.save_table("module_runs_log.csv", module_runs_log, MODULE_RUNS_LOG_HEADERS)
    billing.save_table("github_releases_map.csv", rel_map, GITHUB_RELEASES_MAP_HEADERS)
    billing.save_table("github_assets_map.csv", asset_map, GITHUB_ASSETS_MAP_HEADERS)

    billing.write_state_manifest()
