from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..utils.csvio import read_csv, write_csv
from ..utils.ids import reason_code, validate_category_id, validate_module_id, validate_reason_id
from ..utils.time import utcnow_iso
from ..utils.yamlio import read_yaml


@dataclass
class MaintenanceContext:
    repo_root: Path

    @property
    def modules_dir(self) -> Path:
        return self.repo_root / "modules"

    @property
    def tenants_dir(self) -> Path:
        return self.repo_root / "tenants"

    @property
    def ms_dir(self) -> Path:
        return self.repo_root / "maintenance-state"

    @property
    def ids_dir(self) -> Path:
        return self.ms_dir / "ids"


def _load_categories(ctx: MaintenanceContext) -> Dict[str, Dict[str, str]]:
    rows = read_csv(ctx.ids_dir / "category_registry.csv")
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        cid = r.get("category_id", "").strip()
        if not cid:
            continue
        out[cid] = r
    return out


def _scan_module_ids(ctx: MaintenanceContext) -> List[str]:
    module_ids: List[str] = []
    if not ctx.modules_dir.exists():
        return module_ids
    for p in sorted(ctx.modules_dir.iterdir()):
        if not p.is_dir():
            continue
        mid = p.name
        try:
            validate_module_id(mid)
        except Exception:
            continue
        module_ids.append(mid)
    return module_ids


def _ensure_module_registry(ctx: MaintenanceContext, module_ids: List[str]) -> List[Dict[str, str]]:
    path = ctx.ids_dir / "module_registry.csv"
    rows = read_csv(path)
    by_id = {r.get("module_id"): r for r in rows if r.get("module_id")}

    for mid in module_ids:
        if mid in by_id:
            continue
        # Default category_id from module.yml if present, else '14' (Validation)
        category_id = "14"
        module_yml = ctx.modules_dir / mid / "module.yml"
        if module_yml.exists():
            m = read_yaml(module_yml)
            # If a module file has a 'category_id' override (optional), honor it.
            maybe = str(m.get("category_id", "")).strip()
            if maybe:
                category_id = maybe
        row = {
            "module_id": mid,
            "category_id": category_id,
            "display_name": f"Module {mid}",
            "module_description": str(read_yaml(ctx.modules_dir / mid / "module.yml").get("description", "")).strip() if module_yml.exists() else "",
            "active": "true",
        }
        rows.append(row)

    # Deterministic order
    rows = sorted(rows, key=lambda r: r.get("module_id", ""))
    write_csv(path, rows, ["module_id", "category_id", "display_name", "module_description", "active"])
    return rows


def _load_global_reasons(ctx: MaintenanceContext) -> Dict[str, Dict[str, str]]:
    y = read_yaml(ctx.repo_root / "config" / "global_reasons.yml")
    reasons = y.get("reasons", []) or []
    out: Dict[str, Dict[str, str]] = {}
    for r in reasons:
        key = str(r.get("reason_key", "")).strip()
        if not key:
            continue
        out[key] = {
            "category_id": str(r.get("category_id", "")).strip(),
            "description": str(r.get("description", "")).strip(),
        }
    return out


def _load_module_validation_reasons(ctx: MaintenanceContext, module_id: str) -> Dict[str, Dict[str, str]]:
    path = ctx.modules_dir / module_id / "validation.yml"
    if not path.exists():
        return {}
    y = read_yaml(path)
    reasons = y.get("reasons", []) or []
    out: Dict[str, Dict[str, str]] = {}
    for r in reasons:
        key = str(r.get("reason_key", "")).strip()
        if not key:
            continue
        out[key] = {
            "category_id": str(r.get("category_id", "")).strip(),
            "description": str(r.get("description", "")).strip(),
        }
    return out


def _allocate_lowest_unused(existing_ids: List[str]) -> str:
    used = {int(x) for x in existing_ids if x.isdigit()}
    for i in range(1, 1000):
        if i not in used:
            return f"{i:03d}"
    raise RuntimeError("No available reason_id in 001-999")


def _ensure_reason_registry(ctx: MaintenanceContext, module_registry: List[Dict[str, str]]) -> List[Dict[str, str]]:
    path = ctx.ids_dir / "reason_registry.csv"
    rows = read_csv(path)

    # Indexes
    by_scope_key: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    for r in rows:
        g = str(r.get("g", "")).strip()
        cat = str(r.get("category_id", "")).strip()
        mod = str(r.get("module_id", "")).strip()
        key = str(r.get("reason_key", "")).strip()
        if g and cat and mod and key:
            by_scope_key[(g, cat, mod, key)] = r

    # Helper: get used reason_ids per (g, cat, mod)
    def used_reason_ids(g: str, cat: str, mod: str) -> List[str]:
        return [str(r.get("reason_id", "")).strip() for r in rows if str(r.get("g", "")).strip() == g and str(r.get("category_id", "")).strip() == cat and str(r.get("module_id", "")).strip() == mod]

    # Global reasons (g=0, module_id=000)
    global_reasons = _load_global_reasons(ctx)
    for reason_key, meta in sorted(global_reasons.items()):
        cat = meta.get("category_id") or "16"
        validate_category_id(cat)
        scope_key = ("0", cat, "000", reason_key)
        if scope_key in by_scope_key:
            continue
        rid = _allocate_lowest_unused(used_reason_ids("0", cat, "000"))
        rows.append({
            "g": "0",
            "category_id": cat,
            "module_id": "000",
            "reason_id": rid,
            "reason_key": reason_key,
            "active": "true",
            "notes": "",
        })

    # Module reasons (g=1)
    for m in module_registry:
        mod = str(m.get("module_id", "")).strip()
        if not mod:
            continue
        default_cat = str(m.get("category_id", "")).strip() or "14"
        validate_category_id(default_cat)
        reasons = _load_module_validation_reasons(ctx, mod)
        for reason_key, meta in sorted(reasons.items()):
            cat = (meta.get("category_id") or "").strip() or default_cat
            validate_category_id(cat)
            scope_key = ("1", cat, mod, reason_key)
            if scope_key in by_scope_key:
                continue
            rid = _allocate_lowest_unused(used_reason_ids("1", cat, mod))
            rows.append({
                "g": "1",
                "category_id": cat,
                "module_id": mod,
                "reason_id": rid,
                "reason_key": reason_key,
                "active": "true",
                "notes": "",
            })

    # Deterministic order
    rows = sorted(rows, key=lambda r: (
        int(str(r.get("g", "0"))),
        str(r.get("category_id", "")),
        str(r.get("module_id", "")),
        str(r.get("reason_id", "")),
    ))
    write_csv(path, rows, ["g", "category_id", "module_id", "reason_id", "reason_key", "active", "notes"])
    return rows


def _build_reason_catalog(ctx: MaintenanceContext, categories: Dict[str, Dict[str, str]], module_registry: List[Dict[str, str]], reason_registry: List[Dict[str, str]]) -> List[Dict[str, str]]:
    global_reasons = _load_global_reasons(ctx)
    module_reason_meta: Dict[str, Dict[str, Dict[str, str]]] = {}
    for m in module_registry:
        mod = str(m.get("module_id", "")).strip()
        if not mod:
            continue
        module_reason_meta[mod] = _load_module_validation_reasons(ctx, mod)

    rows: List[Dict[str, str]] = []
    for r in reason_registry:
        g = int(str(r.get("g", "0")).strip() or 0)
        cat = str(r.get("category_id", "")).strip()
        mod = str(r.get("module_id", "")).strip()
        rid = str(r.get("reason_id", "")).strip()
        key = str(r.get("reason_key", "")).strip()
        if not (cat and mod and rid and key):
            continue
        rc = reason_code(g, cat, mod, rid)
        cname = str(categories.get(cat, {}).get("category_name", "")).strip()
        scope = "GLOBAL" if g == 0 else "MODULE"
        desc = ""
        if g == 0:
            desc = global_reasons.get(key, {}).get("description", "")
        else:
            desc = module_reason_meta.get(mod, {}).get(key, {}).get("description", "")
        rows.append({
            "reason_code": rc,
            "g": str(g),
            "category_id": cat,
            "module_id": mod,
            "reason_key": key,
            "category_name": cname,
            "description": desc,
            "scope": scope,
        })

    rows = sorted(rows, key=lambda x: x["reason_code"])
    out_path = ctx.ms_dir / "reason_catalog.csv"
    write_csv(out_path, rows, ["reason_code", "g", "category_id", "module_id", "reason_key", "category_name", "description", "scope"])
    return rows


def _ensure_reason_policy(ctx: MaintenanceContext, reason_catalog: List[Dict[str, str]]) -> List[Dict[str, str]]:
    path = ctx.ms_dir / "reason_policy.csv"
    rows = read_csv(path)
    by_code = {r.get("reason_code"): r for r in rows if r.get("reason_code")}

    for c in reason_catalog:
        code = c.get("reason_code")
        if not code:
            continue
        if code in by_code:
            continue
        g = int(str(c.get("g", "0")))
        key = str(c.get("reason_key", ""))
        refundable = "true" if g == 1 else "false"
        if key in ("internal_error",):
            refundable = "true"
        if key in ("skipped_cache",):
            refundable = "true"
        rows.append({
            "reason_code": code,
            "fail": "true",
            "refundable": refundable,
            "notes": "",
        })

    rows = sorted(rows, key=lambda r: r.get("reason_code", ""))
    write_csv(path, rows, ["reason_code", "fail", "refundable", "notes"])
    return rows


def _build_tenant_relationships(ctx: MaintenanceContext) -> List[Dict[str, str]]:
    out: List[Tuple[str, str]] = []
    if not ctx.tenants_dir.exists():
        return []
    tenants: List[str] = []
    for p in sorted(ctx.tenants_dir.iterdir()):
        if p.is_dir() and (p / "tenant.yml").exists():
            tenants.append(p.name)

    for t in tenants:
        # self pair always
        out.append((t, t))
        y = read_yaml(ctx.tenants_dir / t / "tenant.yml")
        allow = y.get("allow_release_consumers", []) or []
        for source in allow:
            source_t = str(source).strip()
            if not source_t:
                continue
            out.append((source_t, t))

    uniq = sorted(set(out))
    rows = [{"source_tenant_id": s, "target_tenant_id": t} for s, t in uniq]
    path = ctx.ms_dir / "tenant_relationships.csv"
    write_csv(path, rows, ["source_tenant_id", "target_tenant_id"])
    return rows


def _build_module_dependency_index(ctx: MaintenanceContext, module_ids: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for mid in module_ids:
        module_yml = ctx.modules_dir / mid / "module.yml"
        depends: List[str] = []
        notes = ""
        if module_yml.exists():
            y = read_yaml(module_yml)
            depends = [str(x) for x in (y.get("depends_on") or [])]
        # Normalize
        depends = sorted({d for d in depends if d})
        rows.append({
            "module_id": mid,
            "depends_on_module_ids": json.dumps(depends),
            "notes": notes,
        })
    path = ctx.ms_dir / "module_dependency_index.csv"
    write_csv(path, rows, ["module_id", "depends_on_module_ids", "notes"])
    return rows


def _ensure_module_artifacts_policy(ctx: MaintenanceContext, module_ids: List[str]) -> List[Dict[str, str]]:
    path = ctx.ms_dir / "module_artifacts_policy.csv"
    rows = read_csv(path)
    by_mid = {r.get("module_id"): r for r in rows if r.get("module_id")}
    for mid in module_ids:
        if mid in by_mid:
            continue
        rows.append({"module_id": mid, "platform_artifacts_enabled": "true"})
    rows = sorted(rows, key=lambda r: r.get("module_id", ""))
    write_csv(path, rows, ["module_id", "platform_artifacts_enabled"])
    return rows


def _ensure_requirements_index(ctx: MaintenanceContext) -> None:
    path = ctx.ms_dir / "module_requirements_index.csv"
    if path.exists():
        return
    write_csv(path, [], ["module_id", "requirement_type", "requirement_key", "version_or_hash", "source_uri", "cache_group"])


def _write_maintenance_manifest(ctx: MaintenanceContext, module_ids: List[str]) -> None:
    path = ctx.ms_dir / "maintenance_manifest.csv"
    rows = [
        {
            "maintenance_state_version": "v1",
            "updated_at": utcnow_iso(),
            "modules": json.dumps(module_ids),
        }
    ]
    write_csv(path, rows, ["maintenance_state_version", "updated_at", "modules"])


def run_maintenance(repo_root: Path) -> None:
    ctx = MaintenanceContext(repo_root=repo_root)
    categories = _load_categories(ctx)
    module_ids = _scan_module_ids(ctx)
    module_registry = _ensure_module_registry(ctx, module_ids)
    reason_registry = _ensure_reason_registry(ctx, module_registry)
    reason_catalog = _build_reason_catalog(ctx, categories, module_registry, reason_registry)
    _ensure_reason_policy(ctx, reason_catalog)
    _build_tenant_relationships(ctx)
    _build_module_dependency_index(ctx, module_ids)
    _ensure_requirements_index(ctx)
    _ensure_module_artifacts_policy(ctx, module_ids)
    _write_maintenance_manifest(ctx, module_ids)
