# Generated. Do not edit by hand.
CHUNK = r'''\
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

'''

def get_part() -> str:
    return CHUNK
