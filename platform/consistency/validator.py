from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from ..common.id_policy import validate_id
from ..common.id_codec import canon_module_id, canon_tenant_id
from ..utils.csvio import read_csv


class ConsistencyValidationError(ValueError):
    pass


@dataclass
class RuleRow:
    module_id: str
    module_hash: str
    io: str
    port_scope: str
    field_name: str
    field_id: str
    type: str
    item_type: str
    format: str
    required: bool
    default_json: str
    min_value: str
    max_value: str
    min_length: str
    max_length: str
    min_items: str
    max_items: str
    regex: str
    enum_json: str
    description: str
    examples_json: str
    path: str
    content_schema_json: str
    binding_json: str
    rule_json: str

    @property
    def is_tenant_visible(self) -> bool:
        return (self.port_scope or "").lower() == "port"

    def enum_values(self) -> Optional[List[Any]]:
        if not self.enum_json:
            return None
        try:
            v = json.loads(self.enum_json)
            return v if isinstance(v, list) else None
        except Exception:
            return None

    def binding_rules(self) -> Optional[Dict[str, Any]]:
        if not self.binding_json:
            return None
        try:
            v = json.loads(self.binding_json)
            return v if isinstance(v, dict) else None
        except Exception:
            return None


def _read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _as_int(s: str) -> Optional[int]:
    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None
    try:
        return int(ss)
    except Exception:
        return None


def _as_float(s: str) -> Optional[float]:
    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None
    try:
        return float(ss)
    except Exception:
        return None


def _is_binding(obj: Any) -> bool:
    return isinstance(obj, dict) and bool(obj.get("from_step")) and bool(obj.get("from_file"))


def _fail(msg: str) -> None:
    raise ConsistencyValidationError(msg)


def _validate_scalar_type(expected: str, value: Any, ctx: str) -> None:
    if expected == "string":
        if not isinstance(value, str):
            _fail(f"{ctx}: expected string")
        return
    if expected == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            _fail(f"{ctx}: expected integer")
        return
    if expected == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            _fail(f"{ctx}: expected number")
        return
    if expected == "boolean":
        if not isinstance(value, bool):
            _fail(f"{ctx}: expected boolean")
        return
    if expected == "object":
        if not isinstance(value, dict):
            _fail(f"{ctx}: expected object")
        return
    if expected == "array":
        if not isinstance(value, list):
            _fail(f"{ctx}: expected array")
        return
    # Unknown: do not block.


def _validate_constraints(rule: RuleRow, value: Any, ctx: str) -> None:
    # Type
    expected = (rule.type or "").strip()
    if expected:
        _validate_scalar_type(expected, value, ctx)

    # Enum
    enum = rule.enum_values()
    if enum is not None and value not in enum:
        _fail(f"{ctx}: value not in enum {enum}")

    # String constraints
    if isinstance(value, str):
        mn = _as_int(rule.min_length)
        mx = _as_int(rule.max_length)
        if mn is not None and len(value) < mn:
            _fail(f"{ctx}: string length {len(value)} < min_length {mn}")
        if mx is not None and len(value) > mx:
            _fail(f"{ctx}: string length {len(value)} > max_length {mx}")
        if rule.regex:
            try:
                if re.search(rule.regex, value) is None:
                    _fail(f"{ctx}: string does not match regex {rule.regex!r}")
            except re.error:
                # Bad regex in servicing table should not silently pass.
                _fail(f"{ctx}: invalid regex in rules table")

    # Numeric constraints
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        mnv = _as_float(rule.min_value)
        mxv = _as_float(rule.max_value)
        if mnv is not None and float(value) < mnv:
            _fail(f"{ctx}: value {value} < min_value {mnv}")
        if mxv is not None and float(value) > mxv:
            _fail(f"{ctx}: value {value} > max_value {mxv}")

    # Array constraints
    if isinstance(value, list):
        mni = _as_int(rule.min_items)
        mxi = _as_int(rule.max_items)
        if mni is not None and len(value) < mni:
            _fail(f"{ctx}: array length {len(value)} < min_items {mni}")
        if mxi is not None and len(value) > mxi:
            _fail(f"{ctx}: array length {len(value)} > max_items {mxi}")
        it = (rule.item_type or "").strip()
        if it:
            for i, item in enumerate(value):
                _validate_scalar_type(it, item, f"{ctx}[{i}]")


def load_rules_table(repo_root: Path) -> Dict[str, List[RuleRow]]:
    path = repo_root / "maintenance-state" / "module_contract_rules.csv"
    if not path.exists():
        _fail(f"Missing servicing table: {path}")
    rows = read_csv(path)
    out: Dict[str, List[RuleRow]] = {}
    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        rr = RuleRow(
            module_id=mid,
            module_hash=str(r.get("module_hash", "") or ""),
            io=str(r.get("io", "") or ""),
            port_scope=str(r.get("port_scope", "") or ""),
            field_name=str(r.get("field_name", "") or ""),
            field_id=str(r.get("field_id", "") or ""),
            type=str(r.get("type", "") or ""),
            item_type=str(r.get("item_type", "") or ""),
            format=str(r.get("format", "") or ""),
            required=str(r.get("required", "") or "").strip().lower() == "true",
            default_json=str(r.get("default_json", "") or ""),
            min_value=str(r.get("min_value", "") or ""),
            max_value=str(r.get("max_value", "") or ""),
            min_length=str(r.get("min_length", "") or ""),
            max_length=str(r.get("max_length", "") or ""),
            min_items=str(r.get("min_items", "") or ""),
            max_items=str(r.get("max_items", "") or ""),
            regex=str(r.get("regex", "") or ""),
            enum_json=str(r.get("enum_json", "") or ""),
            description=str(r.get("description", "") or ""),
            examples_json=str(r.get("examples_json", "") or ""),
            path=str(r.get("path", "") or ""),
            content_schema_json=str(r.get("content_schema_json", "") or ""),
            binding_json=str(r.get("binding_json", "") or ""),
            rule_json=str(r.get("rule_json", "") or ""),
        )
        out.setdefault(mid, []).append(rr)
    return out


def _index_module_rules(rules: List[RuleRow]) -> Tuple[Dict[str, RuleRow], Dict[str, RuleRow], Set[str]]:
    """Return (tenant_inputs, all_inputs, exposed_output_paths)."""
    inputs: Dict[str, RuleRow] = {}
    tenant_inputs: Dict[str, RuleRow] = {}
    exposed_outputs: Set[str] = set()
    for r in rules:
        if r.io.upper() == "INPUT":
            if r.field_id:
                inputs[r.field_id] = r
                if r.is_tenant_visible:
                    tenant_inputs[r.field_id] = r
        elif r.io.upper() == "OUTPUT":
            if r.is_tenant_visible and r.path:
                exposed_outputs.add(r.path.lstrip("/").strip())
    return tenant_inputs, inputs, exposed_outputs


def validate_workorder(repo_root: Path, workorder_path: Path, module_rules_by_id: Dict[str, List[RuleRow]]) -> None:
    data = _read_yaml(workorder_path)
    if not isinstance(data, dict):
        _fail(f"Invalid YAML: {workorder_path}")
    enabled = bool(data.get("enabled", True))
    if not enabled:
        return

    tid = canon_tenant_id(data.get("tenant_id") or workorder_path.parent.parent.name)
    if tid:
        validate_id("tenant_id", tid, "tenant_id")

    steps = data.get("steps")
    if not isinstance(steps, list) or not steps:
        _fail(f"{workorder_path}: workorder must define non-empty steps list")

    # Collect step IDs and module IDs.
    step_ids: Set[str] = set()
    step_module: Dict[str, str] = {}
    for s in steps:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("step_id") or "").strip()
        mid = canon_module_id(s.get("module_id") or "")
        if not sid or not mid:
            continue
        validate_id("step_id", sid, "workorder.step.step_id")
        validate_id("module_id", mid, "workorder.step.module_id")
        if sid in step_ids:
            _fail(f"{workorder_path}: duplicate step_id {sid!r}")
        step_ids.add(sid)
        step_module[sid] = mid

    # Precompute exposed outputs per step (from servicing table).
    step_outputs: Dict[str, Set[str]] = {}
    for sid, mid in step_module.items():
        rules = module_rules_by_id.get(mid)
        if not rules:
            _fail(f"{workorder_path}: missing module rules for module_id {mid!r} (run Maintenance)")
        _, _, exposed = _index_module_rules(rules)
        step_outputs[sid] = exposed

    # Validate step inputs against module rules.
    for s in steps:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("step_id") or "").strip()
        mid = canon_module_id(s.get("module_id") or "")
        if not sid or not mid:
            continue
        rules = module_rules_by_id.get(mid) or []
        tenant_inputs, all_inputs, _ = _index_module_rules(rules)

        inputs_obj = s.get("inputs") or {}
        if not isinstance(inputs_obj, dict):
            _fail(f"{workorder_path}: step {sid!r} inputs must be an object")

        # Reject tenant-provided limited_port inputs.
        for k in inputs_obj.keys():
            if str(k) not in all_inputs:
                _fail(f"{workorder_path}: step {sid!r} module {mid} has unknown input {k!r}")
            rr = all_inputs[str(k)]
            if not rr.is_tenant_visible:
                _fail(f"{workorder_path}: step {sid!r} input {k!r} is limited_port and must not be set by tenant")

        # Required tenant inputs
        for fid, rr in tenant_inputs.items():
            if rr.required and fid not in inputs_obj:
                _fail(f"{workorder_path}: step {sid!r} missing required input {fid!r} for module {mid}")

        # Validate each provided input value.
        for fid, val in inputs_obj.items():
            fid_s = str(fid)
            rr = all_inputs.get(fid_s)
            if rr is None:
                continue
            ctx = f"{workorder_path}: step {sid!r} input {fid_s!r}"
            if _is_binding(val):
                _validate_binding(val, rr, ctx, step_ids, step_outputs)
            else:
                _validate_constraints(rr, val, ctx)


def _validate_binding(binding: Dict[str, Any], rule: RuleRow, ctx: str, step_ids: Set[str], step_outputs: Dict[str, Set[str]]) -> None:
    br = rule.binding_rules() or {}
    if br and br.get("allowed") is False:
        _fail(f"{ctx}: bindings are not allowed for this field")

    from_step = str(binding.get("from_step") or "").strip()
    from_file = str(binding.get("from_file") or "").lstrip("/").strip()
    selector = str(binding.get("selector") or "").strip()
    take = binding.get("take", None)
    json_path = str(binding.get("json_path") or "").strip() if "json_path" in binding else ""

    if not from_step:
        _fail(f"{ctx}: binding.from_step is required")
    if from_step not in step_ids:
        _fail(f"{ctx}: binding.from_step {from_step!r} not found in steps")
    if not from_file:
        _fail(f"{ctx}: binding.from_file is required")
    allowed_files = step_outputs.get(from_step) or set()
    if allowed_files and from_file not in allowed_files:
        _fail(f"{ctx}: binding.from_file {from_file!r} not exposed by step {from_step!r} (allowed: {sorted(allowed_files)})")
    if not selector:
        _fail(f"{ctx}: binding.selector is required")

    # Field-specific selector rules.
    allowed_selectors = br.get("allowed_selectors") or []
    if allowed_selectors and selector not in allowed_selectors:
        _fail(f"{ctx}: selector {selector!r} not allowed (allowed: {allowed_selectors})")

    sel_rules = (br.get("selector_rules") or {}).get(selector) if isinstance(br.get("selector_rules"), dict) else None
    supports_take = bool(sel_rules.get("supports_take")) if isinstance(sel_rules, dict) else False
    supports_json_path = bool(sel_rules.get("supports_json_path")) if isinstance(sel_rules, dict) else False
    max_take = sel_rules.get("max_take") if isinstance(sel_rules, dict) else None

    if take is not None:
        if not supports_take:
            _fail(f"{ctx}: selector {selector!r} does not support take")
        if not isinstance(take, int) or isinstance(take, bool) or take < 1:
            _fail(f"{ctx}: take must be a positive integer")
        if isinstance(max_take, int) and take > max_take:
            _fail(f"{ctx}: take {take} exceeds max_take {max_take} for selector {selector!r}")

    if json_path:
        if not supports_json_path:
            _fail(f"{ctx}: selector {selector!r} does not support json_path")


def validate_all_workorders(repo_root: Path) -> None:
    rules = load_rules_table(repo_root)
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    any_validated = False
    for td in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not td.is_dir():
            continue
        tenant_id = canon_tenant_id(td.name)
        if tenant_id:
            validate_id("tenant_id", tenant_id, "tenant_id")
        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml")):
            validate_workorder(repo_root, wp, rules)
            any_validated = True

    if not any_validated:
        # Not an error: repo may be scaffold-only.
        return
