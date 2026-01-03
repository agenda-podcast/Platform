from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml

from ..common.id_policy import generate_unique_id, validate_id
from ..common.id_codec import canon_module_id, canon_tenant_id
from ..utils.csvio import read_csv, write_csv
from ..utils.time import utcnow_iso


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

    @property
    def config_dir(self) -> Path:
        return self.repo_root / "config"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]], header: List[str]) -> None:
    write_csv(path, rows, header)


def _default_category_registry() -> List[Dict[str, str]]:
    return [
        {"category_id": "01", "category_name": "Acquisition"},
        {"category_id": "12", "category_name": "Cache"},
        {"category_id": "14", "category_name": "Validation"},
        {"category_id": "15", "category_name": "Access Control"},
        {"category_id": "16", "category_name": "Billing"},
        {"category_id": "99", "category_name": "Other"},
    ]


def _ensure_category_registry(ctx: MaintenanceContext) -> Dict[str, str]:
    _ensure_dir(ctx.ids_dir)
    path = ctx.ids_dir / "category_registry.csv"
    if not path.exists():
        _write_csv(path, _default_category_registry(), ["category_id", "category_name"])
    rows = read_csv(path)
    out: Dict[str, str] = {}
    for r in rows:
        cid = str(r.get("category_id", "")).strip()
        name = str(r.get("category_name", "")).strip()
        if cid:
            out[cid] = name
    return out


def _scan_modules(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not ctx.modules_dir.exists():
        return out

    for p in sorted(ctx.modules_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        mid = p.name.strip()
        validate_id("module_id", mid, "module_id")
        module_yml = p / "module.yml"
        if not module_yml.exists():
            raise FileNotFoundError(str(module_yml))
        data = _read_yaml(module_yml)
        declared = str(data.get("module_id", "")).strip()
        if declared and declared != mid:
            raise ValueError(f"module.yml module_id mismatch: folder={mid} declared={declared}")
        depends = [str(x).strip() for x in (data.get("depends_on") or []) if str(x).strip()]
        for d in depends:
            validate_id("module_id", d, "depends_on_module_id")
        supports_downloadable = bool(data.get("supports_downloadable_artifacts", True))
        out.append({
            "module_id": mid,
            "depends_on": depends,
            "supports_downloadable_artifacts": supports_downloadable,
        })
    return out


def _scan_tenants(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not ctx.tenants_dir.exists():
        return out
    for p in sorted(ctx.tenants_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        tenant_yml = p / "tenant.yml"
        if not tenant_yml.exists():
            continue
        data = _read_yaml(tenant_yml)
        tid = canon_tenant_id(data.get("tenant_id", p.name))
        if not tid:
            continue
        validate_id("tenant_id", tid, "tenant_id")
        consumers = [canon_tenant_id(x) for x in (data.get("allow_release_consumers") or [])]
        consumers = [c for c in consumers if c]
        out.append({"tenant_id": tid, "allow_release_consumers": consumers})
    return out


def _load_global_reasons(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    cfg = _read_yaml(ctx.config_dir / "global_reasons.yml")
    return list(cfg.get("reasons") or [])


def _load_module_reasons(ctx: MaintenanceContext, module_id: str) -> List[Dict[str, Any]]:
    vpath = ctx.modules_dir / module_id / "validation.yml"
    if not vpath.exists():
        return []
    cfg = _read_yaml(vpath)
    return list(cfg.get("reasons") or [])


def _normalize_reason(scope: str, module_id: str, raw: Dict[str, Any]) -> Dict[str, str]:
    rk = str(raw.get("reason_key", "")).strip()
    rs = str(raw.get("reason_slug", "")).strip()
    desc = str(raw.get("description", "")).strip()
    cat = str(raw.get("category_id", "")).strip() or ("16" if scope == "GLOBAL" else "01")

    validate_id("reason_key", rk, "reason_key")
    if not rs:
        raise ValueError("Missing reason_slug")
    if not desc:
        raise ValueError(f"Missing description for reason_slug={rs}")
    if not (len(cat) == 2 and cat.isdigit()):
        raise ValueError(f"Invalid category_id for reason_slug={rs}: {cat!r} (expected 2 digits)")

    if scope == "GLOBAL":
        mod = ""
    else:
        validate_id("module_id", module_id, "module_id")
        mod = module_id

    return {
        "scope": scope,
        "module_id": mod,
        "reason_key": rk,
        "reason_slug": rs,
        "category_id": cat,
        "description": desc,
    }


def _collect_reasons(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in _load_global_reasons(ctx):
        out.append(_normalize_reason("GLOBAL", "", r))

    for m in modules:
        mid = m["module_id"]
        for r in _load_module_reasons(ctx, mid):
            out.append(_normalize_reason("MODULE", mid, r))

    # Uniqueness guarantees:
    seen_keys: set[str] = set()
    seen_slugs: set[Tuple[str, str, str]] = set()
    for r in out:
        rk = r["reason_key"]
        if rk in seen_keys:
            raise ValueError(f"Duplicate reason_key in config/validation: {rk}")
        seen_keys.add(rk)

        key = (r["scope"], r["module_id"], r["reason_slug"])
        if key in seen_slugs:
            raise ValueError(f"Duplicate reason_slug within scope/module: {key}")
        seen_slugs.add(key)

    return out


def _ensure_reason_registry(ctx: MaintenanceContext, reasons: List[Dict[str, str]]) -> List[Dict[str, str]]:
    _ensure_dir(ctx.ids_dir)
    path = ctx.ids_dir / "reason_registry.csv"
    existing = read_csv(path) if path.exists() else []
    by_scope_mod_slug: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    used_codes: set[str] = set()

    for r in existing:
        code = str(r.get("reason_code", "")).strip()
        scope = str(r.get("scope", "")).strip()
        mod = str(r.get("module_id", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        if not (code and scope and slug):
            continue
        try:
            validate_id("reason_code", code, "reason_code")
        except Exception:
            continue
        if scope == "GLOBAL":
            mod = ""
        key = (scope, mod, slug)
        if key in by_scope_mod_slug:
            continue
        used_codes.add(code)
        by_scope_mod_slug[key] = dict(r)

    out: List[Dict[str, str]] = []
    for r in reasons:
        key = (r["scope"], r["module_id"], r["reason_slug"])
        row = by_scope_mod_slug.get(key)
        if row:
            row.update({
                "scope": r["scope"],
                "module_id": r["module_id"],
                "reason_key": r["reason_key"],
                "reason_slug": r["reason_slug"],
                "category_id": r["category_id"],
                "description": r["description"],
            })
        else:
            code = generate_unique_id("reason_code", used_codes)
            row = {
                "reason_code": code,
                "scope": r["scope"],
                "module_id": r["module_id"],
                "reason_key": r["reason_key"],
                "reason_slug": r["reason_slug"],
                "category_id": r["category_id"],
                "description": r["description"],
            }
        out.append(row)

    out = sorted(out, key=lambda x: x["reason_code"])
    _write_csv(path, out, ["reason_code","scope","module_id","reason_key","reason_slug","category_id","description"])
    return out


def _ensure_reason_policy(ctx: MaintenanceContext, reason_registry: List[Dict[str, str]]) -> List[Dict[str, str]]:
    path = ctx.ms_dir / "reason_policy.csv"
    existing = read_csv(path) if path.exists() else []
    by_code: Dict[str, Dict[str, str]] = {str(r.get("reason_code", "")).strip(): r for r in existing if r.get("reason_code")}
    out: List[Dict[str, str]] = []

    for r in reason_registry:
        code = str(r["reason_code"]).strip()
        scope = str(r["scope"]).strip()
        default_ref = "true" if scope == "MODULE" else "false"
        row = by_code.get(code, {})
        refundable = str(row.get("refundable", default_ref)).lower()
        if refundable not in ("true", "false"):
            refundable = default_ref
        out.append({
            "reason_code": code,
            "refundable": refundable,
            "notes": str(row.get("notes", "") or ""),
        })

    out = sorted(out, key=lambda x: x["reason_code"])
    _write_csv(path, out, ["reason_code","refundable","notes"])
    return out


def _write_reason_catalog(ctx: MaintenanceContext, reason_registry: List[Dict[str, str]], categories: Dict[str, str]) -> None:
    path = ctx.ms_dir / "reason_catalog.csv"
    rows: List[Dict[str, str]] = []
    for r in reason_registry:
        cat = str(r.get("category_id", "")).strip()
        rows.append({
            "reason_code": str(r.get("reason_code", "")).strip(),
            "scope": str(r.get("scope", "")).strip(),
            "module_id": str(r.get("module_id", "")).strip(),
            "reason_key": str(r.get("reason_key", "")).strip(),
            "reason_slug": str(r.get("reason_slug", "")).strip(),
            "category_id": cat,
            "category_name": categories.get(cat, ""),
            "description": str(r.get("description", "")).strip(),
        })
    rows = sorted(rows, key=lambda x: x["reason_code"])
    _write_csv(path, rows, ["reason_code","scope","module_id","reason_key","reason_slug","category_id","category_name","description"])


def _write_tenant_relationships(ctx: MaintenanceContext, tenants: List[Dict[str, Any]]) -> None:
    rows: List[Dict[str, str]] = []
    for t in tenants:
        src = t["tenant_id"]
        rows.append({"source_tenant_id": src, "target_tenant_id": src})
        for dst in t.get("allow_release_consumers") or []:
            if dst:
                rows.append({"source_tenant_id": src, "target_tenant_id": dst})
    seen = set()
    deduped=[]
    for r in rows:
        k=(r["source_tenant_id"], r["target_tenant_id"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    deduped = sorted(deduped, key=lambda x: (x["source_tenant_id"], x["target_tenant_id"]))
    _write_csv(ctx.ms_dir / "tenant_relationships.csv", deduped, ["source_tenant_id","target_tenant_id"])


def _write_module_dependency_index(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> None:
    rows: List[Dict[str, str]] = []
    module_ids = {m["module_id"] for m in modules}
    for m in modules:
        mid = m["module_id"]
        for dep in m.get("depends_on") or []:
            if dep not in module_ids:
                raise ValueError(f"module {mid} depends_on unknown module_id {dep}")
            rows.append({"module_id": mid, "depends_on_module_id": dep})
    rows = sorted(rows, key=lambda x: (x["module_id"], x["depends_on_module_id"]))
    _write_csv(ctx.ms_dir / "module_dependency_index.csv", rows, ["module_id","depends_on_module_id"])


def _write_module_requirements_index(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> None:
    src = ctx.repo_root / "platform" / "modules" / "requirements.csv"
    rows = read_csv(src)
    module_ids = {m["module_id"] for m in modules}
    out=[]
    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        if mid not in module_ids:
            raise ValueError(f"requirements.csv references unknown module_id {mid}")
        out.append({
            "module_id": mid,
            "requirement_type": str(r.get("requirement_type","")).strip(),
            "requirement_key": str(r.get("requirement_key","")).strip(),
            "requirement_value": str(r.get("requirement_value","")).strip(),
            "note": str(r.get("note","")).strip(),
        })
    _write_csv(ctx.ms_dir / "module_requirements_index.csv", out,
               ["module_id","requirement_type","requirement_key","requirement_value","note"])


def _write_module_artifacts_policy(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> None:
    cfg = _read_yaml(ctx.config_dir / "platform_policy.yml")
    default_enabled = bool(cfg.get("platform_artifacts_enabled_default", True))
    rows=[]
    for m in modules:
        enabled = default_enabled and bool(m.get("supports_downloadable_artifacts", True))
        rows.append({
            "module_id": m["module_id"],
            "platform_artifacts_enabled": "true" if enabled else "false",
            "notes": "",
        })
    rows = sorted(rows, key=lambda x: x["module_id"])
    _write_csv(ctx.ms_dir / "module_artifacts_policy.csv", rows, ["module_id","platform_artifacts_enabled","notes"])


def _write_platform_policy(ctx: MaintenanceContext) -> None:
    cfg = _read_yaml(ctx.config_dir / "platform_policy.yml")
    rows=[]
    for k,v in cfg.items():
        rows.append({"policy_key": str(k), "policy_value": json.dumps(v) if isinstance(v,(dict,list,bool)) else str(v)})
    _write_csv(ctx.ms_dir / "platform_policy.csv", rows, ["policy_key","policy_value"])


def _write_manifest(ctx: MaintenanceContext) -> None:
    files = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "module_dependency_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "platform_policy.csv",
    ]
    rows=[]
    for fn in files:
        p = ctx.ms_dir / fn
        rows.append({"file": fn, "sha256": _sha256_file(p), "updated_at": utcnow_iso()})
    _write_csv(ctx.ms_dir / "maintenance_manifest.csv", rows, ["file","sha256","updated_at"])


def run_maintenance(repo_root: Path) -> None:
    ctx = MaintenanceContext(repo_root=repo_root)
    _ensure_dir(ctx.ms_dir)
    categories = _ensure_category_registry(ctx)

    modules = _scan_modules(ctx)
    tenants = _scan_tenants(ctx)

    reasons = _collect_reasons(ctx, modules)
    reason_registry = _ensure_reason_registry(ctx, reasons)

    _write_reason_catalog(ctx, reason_registry, categories)
    _ensure_reason_policy(ctx, reason_registry)

    _write_tenant_relationships(ctx, tenants)
    _write_module_dependency_index(ctx, modules)
    _write_module_requirements_index(ctx, modules)
    _write_module_artifacts_policy(ctx, modules)
    _write_platform_policy(ctx)
    _write_manifest(ctx)
