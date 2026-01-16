from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")) or {}


def _sha256_bytes(parts: List[bytes]) -> str:
    h = hashlib.sha256()
    for b in parts:
        h.update(b)
    return h.hexdigest()


def _module_contract_sources(ctx: MaintenanceContext, module_id: str) -> List[Path]:
    """Return the list of files that define a module's *contract*.

    This intentionally excludes runtime code so Maintenance can avoid rewriting
    servicing tables when only implementation changes.
    """
    mid = canon_module_id(module_id)
    if not mid:
        return []
    mdir = ctx.modules_dir / mid
    sources: List[Path] = []
    # Always include module.yml.
    p = mdir / "module.yml"
    if p.exists():
        sources.append(p)

    # Include output schema when present (used for chaining/UI rules).
    osch = mdir / "output_schema.json"
    if osch.exists():
        sources.append(osch)

    return sources


def _compute_module_hash(ctx: MaintenanceContext, module_id: str) -> str:
    parts: List[bytes] = []
    for p in _module_contract_sources(ctx, module_id):
        parts.append(p.read_bytes())
    return _sha256_bytes(parts)


def _json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _schema_primary_type(t: Any) -> Optional[str]:
    """Normalize JSON Schema 'type' to a single primary type.

    If type is a list and includes null, prefer the first non-null.
    """
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        for x in t:
            if x != "null":
                return str(x)
    return None


def _schema_item_type(items: Any) -> Optional[str]:
    if not isinstance(items, dict):
        return None
    return _schema_primary_type(items.get("type"))


def _extract_schema_rules(schema: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], set[str]]:
    """Extract per-property constraints from a tenant params JSON schema."""
    props = schema.get("properties") or {}
    required = schema.get("required") or []
    req_set = {str(x) for x in required if str(x)}
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(props, dict):
        return out, req_set

    for k, v in props.items():
        if not isinstance(v, dict):
            continue
        out[str(k)] = dict(v)
    return out, req_set


def _default_binding_rules(field_type: Optional[str], item_type: Optional[str], max_items: Optional[int]) -> Dict[str, Any]:
    """Conservative binding rules used by Consistency Validation and UI.

    The validator is data-driven per field, so these are only defaults.
    """
    # Arrays of strings: allow reading lines.
    if field_type == "array" and item_type == "string":
        rules: Dict[str, Any] = {
            "allowed": True,
            "object_shape": ["from_step", "from_file", "selector"],
            "allowed_selectors": ["lines"],
            "selector_rules": {
                "lines": {
                    "supports_take": True,
                    "max_take": max_items,
                    "supports_json_path": False,
                }
            },
        }
        return rules

    # Strings: allow text and JSON extraction from json/jsonl_first.
    if field_type == "string":
        return {
            "allowed": True,
            "object_shape": ["from_step", "from_file", "selector"],
            "allowed_selectors": ["text", "json", "jsonl_first"],
            "selector_rules": {
                "text": {"supports_take": False, "supports_json_path": False},
                "json": {"supports_take": False, "supports_json_path": True},
                "jsonl_first": {"supports_take": False, "supports_json_path": True},
            },
        }

    # Objects: allow json/jsonl_first.
    if field_type == "object":
        return {
            "allowed": True,
            "object_shape": ["from_step", "from_file", "selector"],
            "allowed_selectors": ["json", "jsonl_first"],
            "selector_rules": {
                "json": {"supports_take": False, "supports_json_path": True},
                "jsonl_first": {"supports_take": False, "supports_json_path": True},
            },
        }

    # Arrays of objects: allow jsonl.
    if field_type == "array" and item_type == "object":
        return {
            "allowed": True,
            "object_shape": ["from_step", "from_file", "selector"],
            "allowed_selectors": ["jsonl"],
            "selector_rules": {
                "jsonl": {"supports_take": True, "max_take": max_items, "supports_json_path": False},
            },
        }

    # Fallback: allow binding but without selector restrictions (UI can refine later).
    return {
        "allowed": True,
        "object_shape": ["from_step", "from_file", "selector"],
        "allowed_selectors": ["text", "json", "jsonl_first", "jsonl", "lines"],
        "selector_rules": {},
    }


def _compile_module_contract_rules(ctx: MaintenanceContext, module_id: str) -> List[Dict[str, Any]]:
    """Compile the per-module rules rows for module_contract_rules.csv.

    Source of truth: modules/<module_id>/module.yml only.
    No platform-level schemas are consulted.
    """
    mid = canon_module_id(module_id)
    if not mid:
        return []
    mdir = ctx.modules_dir / mid
    myml = _read_yaml(mdir / "module.yml")
    ports = myml.get("ports") or {}
    if not isinstance(ports, dict):
        raise ValueError(f"Invalid ports for module {mid}")
    p_inputs = ports.get("inputs") or {}
    p_outputs = ports.get("outputs") or {}
    if not isinstance(p_inputs, dict) or not isinstance(p_outputs, dict):
        raise ValueError(f"Invalid ports.inputs/ports.outputs for module {mid}")
    in_port = p_inputs.get("port") or []
    in_limited = p_inputs.get("limited_port") or []
    out_port = p_outputs.get("port") or []
    out_limited = p_outputs.get("limited_port") or []
    if not all(isinstance(x, list) for x in (in_port, in_limited, out_port, out_limited)):
        raise ValueError(f"ports.*.port and ports.*.limited_port must be lists for module {mid}")

    # Output schema: optional; used only to enrich content_schema_json.
    output_schema = _read_json(mdir / "output_schema.json") if (mdir / "output_schema.json").exists() else {}
    out_props = (output_schema.get("properties") or {}) if isinstance(output_schema.get("properties"), dict) else {}

    module_hash = _compute_module_hash(ctx, mid)
    rows: List[Dict[str, Any]] = []

    def _compile_inputs(port_scope: str, plist: List[Dict[str, Any]]) -> None:
        for p in plist:
            if not isinstance(p, dict):
                continue
            fid = str(p.get("id") or "").strip()
            if not fid:
                continue
            ptype = str(p.get("type") or "string").strip()
            itype = str(p.get("item_type") or "").strip()
            fmt = str(p.get("format") or "").strip()
            desc = str(p.get("description") or "").strip()
            required = bool(p.get("required", False))
            default_val = p.get("default", None)
            schema = p.get("schema") or {}
            minimum = schema.get("minimum")
            maximum = schema.get("maximum")
            min_len = schema.get("minLength")
            max_len = schema.get("maxLength")
            min_items = schema.get("minItems")
            max_items = schema.get("maxItems")
            pattern = schema.get("pattern")
            enum = schema.get("enum")
            examples = schema.get("examples")
            custom_binding = p.get("binding")
            if isinstance(custom_binding, dict) and custom_binding:
                binding_rules = custom_binding
            else:
                binding_rules = _default_binding_rules(ptype, itype or None, int(max_items) if isinstance(max_items, int) else None)
            rule_obj = {"io": "input", "id": fid, "schema": schema, "binding": binding_rules}

            rows.append(
                {
                    "module_id": mid,
                    "module_hash": module_hash,
                    "io": "INPUT",
                    "port_scope": port_scope,
                    "field_name": f"inputs.{fid}",
                    "field_id": fid,
                    "type": ptype,
                    "item_type": itype,
                    "format": fmt,
                    "required": "true" if required else "false",
                    "default_json": "" if default_val is None else _json_dumps_compact(default_val),
                    "min_value": "" if minimum is None else str(minimum),
                    "max_value": "" if maximum is None else str(maximum),
                    "min_length": "" if min_len is None else str(min_len),
                    "max_length": "" if max_len is None else str(max_len),
                    "min_items": "" if min_items is None else str(min_items),
                    "max_items": "" if max_items is None else str(max_items),
                    "regex": "" if pattern is None else str(pattern),
                    "enum_json": "" if enum is None else _json_dumps_compact(enum),
                    "description": desc,
                    "examples_json": "" if examples is None else _json_dumps_compact(examples),
                    "path": "",
                    "content_schema_json": "",
                    "binding_json": _json_dumps_compact(binding_rules),
                    "rule_json": _json_dumps_compact(rule_obj),
                    "platform_limit_json": "",
                }
            )

    _compile_inputs("port", in_port)
    _compile_inputs("limited_port", in_limited)

    def _compile_outputs(port_scope: str, plist: List[Dict[str, Any]]) -> None:
        for p in plist:
            if not isinstance(p, dict):
                continue
            fid = str(p.get("id") or "").strip()
            if not fid:
                continue
            otype = str(p.get("type") or "file").strip()
            fmt = str(p.get("format") or "").strip()
            desc = str(p.get("description") or "").strip()
            path = str(p.get("path") or "").lstrip("/").strip()

            # Best-effort content schema extraction from output_schema.json.
            content_schema: Any = {}
            if output_schema and path:
                if fid == "report" and isinstance(out_props.get("report_schema"), dict):
                    content_schema = out_props.get("report_schema")
                elif "jsonlines" in fmt and isinstance(out_props.get("results_line_schema"), dict):
                    content_schema = out_props.get("results_line_schema")
                elif isinstance(out_props.get(f"{fid}_schema"), dict):
                    content_schema = out_props.get(f"{fid}_schema")

            rule_obj = {
                "io": "output",
                "id": fid,
                "path": path,
                "format": fmt,
                "content_schema": content_schema if content_schema else None,
            }

            rows.append(
                {
                    "module_id": mid,
                    "module_hash": module_hash,
                    "io": "OUTPUT",
                    "port_scope": port_scope,
                    "field_name": f"outputs.{fid}",
                    "field_id": fid,
                    "type": otype,
                    "item_type": "",
                    "format": fmt,
                    "required": "",
                    "default_json": "",
                    "min_value": "",
                    "max_value": "",
                    "min_length": "",
                    "max_length": "",
                    "min_items": "",
                    "max_items": "",
                    "regex": "",
                    "enum_json": "",
                    "description": desc,
                    "examples_json": "",
                    "path": path,
                    "content_schema_json": "" if not content_schema else _json_dumps_compact(content_schema),
                    "binding_json": "",
                    "rule_json": _json_dumps_compact(rule_obj),
                    "platform_limit_json": "",
                }
            )

    _compile_outputs("port", out_port)
    _compile_outputs("limited_port", out_limited)

    # Stable sort.
    rows = sorted(rows, key=lambda r: (r["module_id"], r["io"], r["port_scope"], r["field_name"]))
    return rows


def _write_module_contract_rules(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> None:
    """Write maintenance-state/module_contract_rules.csv.

    Incremental behavior:
      - If module_hash is unchanged vs existing CSV, keep rows byte-identical.
      - If module_hash changes, regenerate all rows for that module.
    """
    path = ctx.ms_dir / "module_contract_rules.csv"
    existing = read_csv(path) if path.exists() else []
    existing_by_module: Dict[str, List[Dict[str, str]]] = {}
    existing_hash: Dict[str, str] = {}
    for r in existing:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        existing_by_module.setdefault(mid, []).append(r)
        h = str(r.get("module_hash", "")).strip()
        if h:
            existing_hash[mid] = h

    out_rows: List[Dict[str, Any]] = []
    for m in modules:
        mid = m["module_id"]
        new_hash = _compute_module_hash(ctx, mid)
        if existing_hash.get(mid) == new_hash and mid in existing_by_module:
            # Preserve existing rows to keep file stable.
            out_rows.extend(existing_by_module[mid])
        else:
            out_rows.extend(_compile_module_contract_rules(ctx, mid))

    header = [
        "module_id",
        "module_hash",
        "io",
        "port_scope",
        "field_name",
        "field_id",
        "type",
        "item_type",
        "format",
        "required",
        "default_json",
        "min_value",
        "max_value",
        "min_length",
        "max_length",
        "min_items",
        "max_items",
        "regex",
        "enum_json",
        "description",
        "examples_json",
        "path",
        "content_schema_json",
        "binding_json",
        "rule_json",
        "platform_limit_json",
    ]

    # Stable sort for the full file too.
    out_rows = sorted(
        out_rows,
        key=lambda r: (
            str(r.get("module_id", "")),
            str(r.get("io", "")),
            str(r.get("port_scope", "")),
            str(r.get("field_name", "")),
        ),
    )
    _write_csv(path, out_rows, header)


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
        supports_downloadable = bool(data.get("supports_downloadable_artifacts", True))
        out.append({
            "module_id": mid,
            "supports_downloadable_artifacts": supports_downloadable,
        })
    return out




def _write_modules_index(ctx: MaintenanceContext) -> None:
    """Write maintenance-state/modules_index.csv.

    Purpose:
      - provide deterministic, stable module options for UI dropdowns
      - avoid scanning modules/ at runtime for interactive workflows

    Design:
      - stable sort by module_id
      - include commonly used display fields (name, kind, version)
    """
    rows = []
    if not ctx.modules_dir.exists():
        _write_csv(ctx.ms_dir / "modules_index.csv", [], [
            "module_id",
            "name",
            "kind",
            "version",
            "supports_downloadable_artifacts",
            "path",
        ])
        return

    for p in sorted(ctx.modules_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        mid = p.name.strip()
        if not mid:
            continue
        validate_id("module_id", mid, "module_id")

        module_yml = p / "module.yml"
        if not module_yml.exists():
            raise FileNotFoundError(str(module_yml))
        data = _read_yaml(module_yml)
        declared = str(data.get("module_id", "") or "").strip()
        if declared and declared != mid:
            raise ValueError(f"module.yml module_id mismatch: folder={mid} declared={declared}")

        name = str(data.get("name", "") or "").strip()
        kind = str(data.get("kind", "") or "").strip()
        version = str(data.get("version", "") or "").strip()
        supports_downloadable = bool(data.get("supports_downloadable_artifacts", True))

        rows.append({
            "module_id": mid,
            "name": name,
            "kind": kind,
            "version": version,
            "supports_downloadable_artifacts": "true" if supports_downloadable else "false",
            "path": str(p.relative_to(ctx.repo_root)),
        })

    rows = sorted(rows, key=lambda r: (r.get("module_id", ""), r.get("name", "")))
    _write_csv(ctx.ms_dir / "modules_index.csv", rows, [
        "module_id",
        "name",
        "kind",
        "version",
        "supports_downloadable_artifacts",
        "path",
    ])


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


def _write_workorders_index(ctx: MaintenanceContext, tenants: List[Dict[str, Any]]) -> None:
    """Write maintenance-state/workorders_index.csv.

    Purpose:
      - enforce global uniqueness of work_order_id across all tenants
      - provide a centralized queue/index for Orchestrator and preflight workflows
    """
    rows: List[Dict[str, str]] = []
    seen: Dict[str, str] = {}
    for t in tenants:
        tid = str(t.get("tenant_id", "") or "").strip()
        if not tid:
            continue
        wdir = ctx.tenants_dir / tid / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            wo = _read_yaml(wp)
            wid = str(wo.get("work_order_id") or wp.stem).strip()
            if not wid:
                continue
            validate_id("work_order_id", wid, "work_order_id")
            # Enforce global uniqueness across tenants
            if wid in seen and seen[wid] != tid:
                raise ValueError(f"work_order_id must be globally unique: {wid} used by tenants {seen[wid]} and {tid}")
            seen[wid] = tid

            enabled = bool(wo.get("enabled", True))
            meta = wo.get("metadata") or {}
            title = str(meta.get("title", "") or "").strip()
            notes = str(meta.get("notes", "") or "").strip()
            schedule = str(wo.get("schedule_cron", "") or wo.get("cron", "") or "").strip()

            rows.append({
                "tenant_id": tid,
                "work_order_id": wid,
                "enabled": "true" if enabled else "false",
                "schedule_cron": schedule,
                "title": title,
                "notes": notes,
                "path": str(wp.relative_to(ctx.repo_root)),
            })

    rows = sorted(rows, key=lambda r: (r["enabled"] != "true", r["tenant_id"], r["work_order_id"]))
    _write_csv(ctx.ms_dir / "workorders_index.csv", rows, [
        "tenant_id",
        "work_order_id",
        "enabled",
        "schedule_cron",
        "title",
        "notes",
        "path",
    ])


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




def _write_module_requirements_index(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Write module_requirements_index.csv.

    Source of truth: modules/<module_id>/module.yml (requirements block).
    platform/modules/requirements.csv is deprecated and must be header-only.
    """
    legacy = read_csv(ctx.repo_root / "platform" / "modules" / "requirements.csv")
    if legacy:
        raise ValueError(
            "platform/modules/requirements.csv is deprecated and must be header-only. "
            "Move requirements to modules/<module_id>/module.yml under 'requirements:'."
        )

    module_ids = [m["module_id"] for m in modules]
    out: List[Dict[str, str]] = []

    for mid in module_ids:
        mdir = ctx.modules_dir / mid
        myml = _read_yaml(mdir / "module.yml")
        req = myml.get("requirements") or {}
        if not isinstance(req, dict):
            continue

        def _emit(req_type: str, item: Any) -> None:
            if isinstance(item, str):
                name = item.strip()
                note = ""
                val = ""
            elif isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                note = str(item.get("note") or "").strip()
                val = "" if item.get("default") is None else str(item.get("default"))
            else:
                return
            if not name:
                return
            out.append(
                {
                    "module_id": mid,
                    "requirement_type": req_type,
                    "requirement_key": name,
                    "requirement_value": val,
                    "note": note,
                }
            )

        for it in (req.get("secrets") or []):
            _emit("secret", it)
        for it in (req.get("vars") or []):
            _emit("var", it)

    _write_csv(
        ctx.ms_dir / "module_requirements_index.csv",
        out,
        ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"],
    )
    return out


def _write_secretstore_template(ctx: MaintenanceContext, modules: List[Dict[str, Any]], req_rows: List[Dict[str, str]]) -> None:
    """Regenerate platform/secretstore/secretstore.template.json from module requirements.

    Template keys are module-scoped. Secret names are allowed to be non-unique across modules.
    Values are placeholders only (safe to commit).
    """
    mods: Dict[str, Dict[str, Any]] = {}
    for m in modules:
        mid = m["module_id"]
        mods[mid] = {"secrets": {}, "vars": {}}

    for r in req_rows:
        mid = (r.get("module_id") or "").strip()
        rtype = (r.get("requirement_type") or "").strip()
        key = (r.get("requirement_key") or "").strip()
        val = r.get("requirement_value")
        if not mid or not key or mid not in mods:
            continue
        if rtype == "secret":
            mods[mid]["secrets"][key] = "REPLACE_ME"
        elif rtype == "var":
            mods[mid]["vars"][key] = val if isinstance(val, str) and val != "" else "REPLACE_ME"

    integrations: Dict[str, Dict[str, Any]] = {}

    # Integration requirements derived from runtime profiles.
    # These are platform-level secrets/vars that are not tied to a module.
    # They live under the top-level "integrations" key in secretstore JSON.
    runtime_profiles = sorted(ctx.config_dir.glob("runtime_profile*.yml"))
    required_integ: Dict[str, Dict[str, List[str]]] = {}

    has_dropbox_delivery = any(str(m.get("module_id", "")).strip() == "deliver_dropbox" for m in modules)

    def _add_integration_req(integration_id: str, *, secrets: List[str], vars: List[str]) -> None:
        blk = required_integ.setdefault(integration_id, {"secrets": [], "vars": []})
        for s in secrets:
            if s not in blk["secrets"]:
                blk["secrets"].append(s)
        for v in vars:
            if v not in blk["vars"]:
                blk["vars"].append(v)

    def _extract_kinds(adapter_obj: Any) -> List[str]:
        if not isinstance(adapter_obj, dict):
            return []
        kind = str(adapter_obj.get("kind", "") or "").strip()
        if kind and kind != "multi":
            return [kind]
        if kind == "multi":
            inner = adapter_obj.get("stores") or adapter_obj.get("publishers") or []
            out: List[str] = []
            if isinstance(inner, list):
                for it in inner:
                    if isinstance(it, dict):
                        k = str(it.get("kind", "") or "").strip()
                        if k:
                            out.append(k)
            return out
        return []

    for rp in runtime_profiles:
        try:
            y = _read_yaml(rp)
        except Exception:
            continue
        adapters = y.get("adapters") or {}
        if not isinstance(adapters, dict):
            continue
        # Artifact store requirements
        kinds = _extract_kinds(adapters.get("artifact_store") or {})
        if "s3" in kinds:
            _add_integration_req(
                "artifact_store_s3",
                secrets=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
                vars=["AWS_DEFAULT_REGION", "PLATFORM_ARTIFACTS_S3_BUCKET", "PLATFORM_ARTIFACTS_S3_PREFIX"],
            )

        # OAuth / tenant credentials requirements
        tcs = adapters.get("tenant_credentials_store")
        if isinstance(tcs, dict) and str(tcs.get("kind", "") or "").strip():
            _add_integration_req(
                "oauth_global",
                secrets=["OAUTH_STATE_SIGNING_KEY", "TOKEN_ENCRYPTION_KEY"],
                vars=[],
            )
            if has_dropbox_delivery:
                _add_integration_req(
                    "oauth_dropbox",
                    secrets=["DROPBOX_APP_KEY", "DROPBOX_APP_SECRET"],
                    vars=["DROPBOX_SCOPES"],
                )

    for integration_id, blk in sorted(required_integ.items(), key=lambda x: x[0]):
        integrations[integration_id] = {"secrets": {}, "vars": {}}
        for k in sorted(blk.get("secrets") or []):
            integrations[integration_id]["secrets"][k] = "REPLACE_ME"
        for k in sorted(blk.get("vars") or []):
            integrations[integration_id]["vars"][k] = "REPLACE_ME"

    payload = {"version": 1, "generated_at": "MAINTENANCE", "modules": mods, "integrations": integrations}
    out_path = ctx.repo_root / "platform" / "secretstore" / "secretstore.template.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

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
    # IMPORTANT: This manifest must be *stable* across runs when underlying
    # file contents have not changed. Otherwise CI will always detect drift.
    #
    # Rule: preserve updated_at if sha256 is unchanged from the prior manifest.
    existing_path = ctx.ms_dir / "maintenance_manifest.csv"
    existing_rows = read_csv(existing_path) if existing_path.exists() else []
    existing_by_file: Dict[str, Dict[str, str]] = {}
    for r in existing_rows:
        fn = str(r.get("file", "")).strip()
        sha = str(r.get("sha256", "")).strip()
        ts = str(r.get("updated_at", "")).strip()
        if fn and sha and ts:
            existing_by_file[fn] = {"sha256": sha, "updated_at": ts}

    files = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "workorders_index.csv",
        "modules_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "module_contract_rules.csv",
        "platform_policy.csv",
    ]
    rows=[]
    for fn in files:
        p = ctx.ms_dir / fn
        sha = _sha256_file(p)
        prior = existing_by_file.get(fn)
        if prior and prior.get("sha256") == sha:
            ts = prior.get("updated_at", "")
        else:
            ts = utcnow_iso()
        rows.append({"file": fn, "sha256": sha, "updated_at": ts})
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
    _write_modules_index(ctx)
    _write_workorders_index(ctx, tenants)
    req_rows = _write_module_requirements_index(ctx, modules)
    _write_secretstore_template(ctx, modules, req_rows)
    _write_module_artifacts_policy(ctx, modules)
    _write_module_contract_rules(ctx, modules)
    _write_platform_policy(ctx)
    _write_manifest(ctx)
