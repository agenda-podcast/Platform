from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

from ..common.id_policy import validate_id
from ..common.id_codec import canon_module_id, canon_tenant_id
from ..utils.csvio import read_csv
from ..infra.models import MODULE_KIND_VALUES, is_valid_module_kind


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
        except Exception as e:
            raise ValueError(f"Invalid enum_json for {self.module_id}:{self.field_id}: {e}")
        return v if isinstance(v, list) else None

    def binding_rules(self) -> Optional[Dict[str, Any]]:
        if not self.binding_json:
            return None
        try:
            v = json.loads(self.binding_json)
        except Exception as e:
            raise ValueError(f"Invalid binding_json for {self.module_id}:{self.field_id}: {e}")
        return v if isinstance(v, dict) else None



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

EMAIL_ATTACHMENT_THRESHOLD_BYTES = 20866662  # 19.9 MiB (deterministic threshold used for email delivery gating)


def _load_module_kind(repo_root: Path, module_id: str) -> str:
    mid = canon_module_id(module_id)
    if not mid:
        _fail(f"Invalid module_id for kind lookup: {module_id!r}")
    yml = repo_root / "modules" / mid / "module.yml"
    if not yml.exists():
        _fail(f"Missing module.yml for module {mid!r}")
    data = _read_yaml(yml)
    if not isinstance(data, dict):
        _fail(f"Invalid module.yml format for {mid!r}")
    kind = str(data.get("kind") or "").strip()
    if not kind:
        _fail(f"module {mid!r} missing required field 'kind' (allowed: {list(MODULE_KIND_VALUES)})")
    if not is_valid_module_kind(kind):
        _fail(f"module {mid!r} has invalid kind={kind!r} (allowed: {list(MODULE_KIND_VALUES)})")
    return kind


def _is_binding(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    has_step = bool(str(obj.get("from_step") or obj.get("step_id") or "").strip())
    if not has_step:
        return False
    # Treat any object that declares a source step as a binding candidate so we can
    # emit a precise error (for example, missing output_id) based on binding rules.
    return True


def _iter_bindings(obj: Any):
    """Yield all binding objects found recursively within obj."""
    if _is_binding(obj):
        yield obj
        return
    if isinstance(obj, dict):
        for v in obj.values():
            yield from _iter_bindings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_bindings(v)


def _validate_binding(val: Any, rr: RuleRow, ctx: str, step_ids: Set[str], step_outputs: Dict[str, Set[str]]) -> None:
    """Validate an input binding reference.

    Binding object forms supported (canonical):
      {"from_step": "<step_id>", "output_id": "<output_id>"}

    Backward-compatible aliases:
      {"step_id": "<step_id>", "output_id": "<output_id>"}
      {"from": {"step_id": "<step_id>", "output_id": "<output_id>"}}
    """
    rules = rr.binding_rules()
    if not rules:
        _fail(f"{ctx}: bindings are not allowed for input {rr.field_id!r} on module {rr.module_id!r}")

    if not isinstance(val, dict):
        _fail(f"{ctx}: binding must be an object")

    src = val
    if isinstance(val.get("from"), dict):
        src = val.get("from") or {}

    from_step = str(src.get("from_step") or src.get("step_id") or "").strip()
    output_id = str(src.get("output_id") or "").strip()
    from_file = str(src.get("from_file") or "").strip()

    if not from_step:
        _fail(f"{ctx}: binding must include from_step")

    require_output_id = bool(rules.get("require_output_id"))
    require_from_file = bool(rules.get("require_from_file"))

    if require_output_id:
        if not output_id:
            _fail(f"{ctx}: binding must include output_id")
        if from_file:
            _fail(f"{ctx}: binding must not include from_file when output_id is required")

    if require_from_file:
        if not from_file:
            _fail(f"{ctx}: binding must include from_file")
        if output_id:
            _fail(f"{ctx}: binding must not include output_id when from_file is required")

    # Two supported binding forms:
    # 1) output_id binding (canonical)
    # 2) from_file binding (file selector binding used by current modules)
    if not output_id and not from_file:
        _fail(f"{ctx}: binding must include output_id or from_file")

    if from_step not in step_ids:
        _fail(f"{ctx}: binding from_step {from_step!r} does not exist in workorder")

    exposed = step_outputs.get(from_step) or set()
    if output_id and output_id not in exposed:
        _fail(f"{ctx}: binding output_id {output_id!r} is not exposed by step {from_step!r}")
    if from_file and from_file not in exposed:
        _fail(f"{ctx}: binding from_file {from_file!r} is not exposed by step {from_step!r}")

    # Optional type check if schema provides constraints.
    if output_id:
        allowed_outputs = rules.get("allowed_outputs")
        if isinstance(allowed_outputs, list) and allowed_outputs:
            allowed_set = {str(x) for x in allowed_outputs if str(x)}
            if output_id not in allowed_set:
                _fail(f"{ctx}: binding output_id {output_id!r} not permitted for input {rr.field_id!r}")


def _fail(msg: str) -> None:
    raise ConsistencyValidationError(msg)


def _parse_json(raw: str, ctx: str) -> Any:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception as e:
        _fail(f"{ctx}: invalid JSON: {e}")


def _parse_default(rule: RuleRow, ctx: str) -> Tuple[bool, Any]:
    if not rule.default_json:
        return (False, None)
    v = _parse_json(rule.default_json, f"{ctx}: default_json")
    return (True, v)


def _normalize_requested_deliverables_for_preflight(repo_root: Path, module_id: str, step_cfg: Dict[str, Any]) -> Tuple[List[str], str]:
    # Mirrors orchestrator deliverables selection (no execution).
    if "deliverables" in step_cfg and step_cfg.get("deliverables") is not None:
        raw = step_cfg.get("deliverables")
        if not isinstance(raw, list):
            _fail(f"step.deliverables must be a list for module {module_id}")
        out: List[str] = []
        seen: Set[str] = set()
        for x in raw:
            s = str(x or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out, "explicit"

    if bool(step_cfg.get("purchase_release_artifacts", False)):
        mid = canon_module_id(module_id)
        contract = _load_module_deliverables_contract(repo_root, mid)
        if "tenant_outputs" in contract:
            return ["tenant_outputs"], "legacy:tenant_outputs"
        if contract:
            return sorted(contract.keys()), "legacy:all"
        return [], "legacy:none"

    return [], "none"


def _load_module_deliverables_contract(repo_root: Path, module_id: str) -> Dict[str, Dict[str, Any]]:
    mid = canon_module_id(module_id)
    if not mid:
        _fail(f"Invalid module_id for deliverables: {module_id!r}")
    yml = repo_root / "modules" / mid / "module.yml"
    if not yml.exists():
        _fail(f"Missing module.yml for module {mid!r}")
    data = _read_yaml(yml)
    if not isinstance(data, dict):
        _fail(f"Invalid module.yml format for {mid!r}")

    # output ids declared by module
    ports = data.get("ports") or {}
    out_ids: Set[str] = set()
    try:
        outputs = (ports.get("outputs") or {}) if isinstance(ports, dict) else {}
        for lst in ((outputs.get("port") or []), (outputs.get("limited_port") or [])):
            if not isinstance(lst, list):
                continue
            for o in lst:
                if not isinstance(o, dict):
                    continue
                oid = str(o.get("id") or "").strip()
                if oid:
                    out_ids.add(oid)
    except Exception:
        out_ids = set()

    deliverables = ((data.get("deliverables") or {}).get("port") or [])
    if deliverables is None:
        deliverables = []
    if not isinstance(deliverables, list):
        _fail(f"deliverables.port must be a list in module.yml for {mid!r}")

    out: Dict[str, Dict[str, Any]] = {}
    for d in deliverables:
        if not isinstance(d, dict):
            continue
        did = str(d.get("deliverable_id") or "").strip()
        if not did:
            continue
        if did in out:
            _fail(f"Duplicate deliverable_id {did!r} in module.yml for {mid!r}")
        lim = d.get("limited_inputs") or {}
        if lim is None:
            lim = {}
        if not isinstance(lim, dict):
            _fail(f"deliverables.port[].limited_inputs must be an object for {mid!r}:{did!r}")
        outputs = d.get("outputs") or []
        if outputs is None:
            outputs = []
        if not isinstance(outputs, list):
            _fail(f"deliverables.port[].outputs must be a list for {mid!r}:{did!r}")
        out_list: List[str] = []
        seen_out: Set[str] = set()
        for oid in outputs:
            oids = str(oid or "").strip()
            if not oids:
                continue
            if oids in seen_out:
                continue
            seen_out.add(oids)
            if out_ids and oids not in out_ids:
                _fail(f"module {mid!r} deliverable {did!r} references unknown output id {oids!r}")
            out_list.append(oids)
        out[did] = {"limited_inputs": {str(k): v for k, v in lim.items()}, "outputs": out_list}

    return out


def _union_limited_inputs(contract: Dict[str, Dict[str, Any]], requested: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for did in requested:
        d = contract.get(did) or {}
        lim = d.get("limited_inputs") or {}
        if isinstance(lim, dict):
            for k, v in lim.items():
                out[str(k)] = v
    return out


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
    # Bindings are validated separately and bypass scalar constraints.
    if _is_binding(value):
        return

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
                if _is_binding(item):
                    continue
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
    """Return (tenant_inputs, all_inputs, exposed_outputs).

    exposed_outputs contains both output IDs and output relative paths for tenant-visible outputs.
    This supports two binding styles:
      - file binding: from_file references an exposed output path
      - output binding: output_id references an exposed output id
    """
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
            if not r.is_tenant_visible:
                continue
            if r.field_id:
                exposed_outputs.add(str(r.field_id).strip())
            if r.path:
                exposed_outputs.add(r.path.lstrip("/").strip())
    return tenant_inputs, inputs, exposed_outputs
def validate_workorder(repo_root: Path, workorder_path: Path, module_rules_by_id: Dict[str, List[RuleRow]]) -> None:
    # Strict validation for enabled workorders.
    # Drafts (enabled=false) are allowed, but preflight may return warnings.
    data = _read_yaml(workorder_path)
    if not isinstance(data, dict):
        _fail(f"Invalid YAML: {workorder_path}")
    enabled = bool(data.get("enabled", True))
    # Always run preflight; it will raise only when enabled=true.
    _ = validate_workorder_preflight(repo_root, workorder_path, module_rules_by_id)
    return


def validate_workorder_preflight(repo_root: Path, workorder_path: Path, module_rules_by_id: Dict[str, List[RuleRow]]) -> Dict[str, Any]:
    # Validate + defaults/enrichment only (no module execution).
    data = _read_yaml(workorder_path)
    if not isinstance(data, dict):
        _fail(f"Invalid YAML: {workorder_path}")

    warnings: List[str] = []
    enabled = bool(data.get("enabled", True))
    artifacts_requested = bool(data.get("artifacts_requested", False))

    tid = canon_tenant_id(data.get("tenant_id") or workorder_path.parent.parent.name)
    if tid:
        validate_id("tenant_id", tid, "tenant_id")

    steps = data.get("steps")
    if not isinstance(steps, list) or not steps:
        if enabled:
            _fail(f"{workorder_path}: workorder must define non-empty steps list")
        warnings.append(f"{workorder_path}: draft warning: missing or empty steps list")
        return {"enabled": False, "path": str(workorder_path), "warnings": warnings}

    # Collect step IDs and module IDs for enabled steps.
    step_ids: Set[str] = set()
    step_module: Dict[str, str] = {}
    step_cfg_by_id: Dict[str, Dict[str, Any]] = {}
    step_kind_by_id: Dict[str, str] = {}
    ordered_enabled_steps: List[str] = []

    for s in steps:
        if not isinstance(s, dict):
            continue
        step_enabled = bool(s.get("enabled", True))
        if not step_enabled:
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
        ordered_enabled_steps.append(sid)
        step_module[sid] = mid
        step_cfg_by_id[sid] = dict(s)

        sk = str(s.get("kind") or "").strip()
        if not sk:
            if enabled:
                _fail(f"{workorder_path}: step {sid!r} missing required field 'kind' (allowed: {list(MODULE_KIND_VALUES)})")
            warnings.append(f"{workorder_path}: draft warning: step {sid!r} missing required field 'kind'")
            continue
        if not is_valid_module_kind(sk):
            if enabled:
                _fail(f"{workorder_path}: step {sid!r} has invalid kind={sk!r} (allowed: {list(MODULE_KIND_VALUES)})")
            warnings.append(f"{workorder_path}: draft warning: step {sid!r} invalid kind={sk!r}")
            continue
        step_kind_by_id[sid] = sk

    # Activation gating (no injection; draft allowed)
    # Rules:
    #  - artifacts_requested=true => packaging + delivery mandatory
    #  - packaging present => delivery mandatory
    #  - delivery present => packaging required and earlier
    #
    # Enabled workorders: failures are blocking.
    # Draft workorders: surface warnings only.
    packaging_steps = [sid for sid in ordered_enabled_steps if step_kind_by_id.get(sid) == "packaging"]
    delivery_steps = [sid for sid in ordered_enabled_steps if step_kind_by_id.get(sid) == "delivery"]

    def _gate_fail_or_warn(msg: str) -> None:
        if enabled:
            _fail(f"{workorder_path}: {msg}")
        warnings.append(f"{workorder_path}: draft warning: {msg}")

    def _step_fail_or_warn(msg: str) -> None:
        """Drafts (enabled=false) must not be blocked by validator.

        We still surface actionable warnings so tenants can iterate safely.
        """
        if enabled:
            _fail(f"{workorder_path}: {msg}")
        warnings.append(f"{workorder_path}: draft warning: {msg}")

    if artifacts_requested:
        if not packaging_steps:
            _gate_fail_or_warn("missing packaging step")
        if not delivery_steps:
            _gate_fail_or_warn("missing delivery step")
        if packaging_steps and delivery_steps:
            first_pack_idx = ordered_enabled_steps.index(packaging_steps[0])
            first_del_idx = ordered_enabled_steps.index(delivery_steps[0])
            if first_del_idx < first_pack_idx:
                _gate_fail_or_warn("wrong order (delivery before packaging)")

    if packaging_steps and not delivery_steps:
        _gate_fail_or_warn("missing delivery step")

    if delivery_steps:
        if not packaging_steps:
            _gate_fail_or_warn("missing packaging step")
        else:
            first_pack_idx = ordered_enabled_steps.index(packaging_steps[0])
            for dsid in delivery_steps:
                didx = ordered_enabled_steps.index(dsid)
                if didx < first_pack_idx:
                    _gate_fail_or_warn("wrong order (delivery before packaging)")
                    break

        # Rule 4: Email delivery requires deterministic size threshold declaration on packaging step.
        for dsid in delivery_steps:
            cfg = step_cfg_by_id.get(dsid) or {}
            method = ""
            if isinstance(cfg.get("delivery"), dict):
                method = str((cfg.get("delivery") or {}).get("method") or "").strip().lower()
            if not method:
                method = str(cfg.get("delivery_method") or "").strip().lower()
            if method != "email":
                continue

            # nearest packaging step before this delivery step
            didx = ordered_enabled_steps.index(dsid)
            prev_pack = None
            for sid in reversed(ordered_enabled_steps[:didx]):
                if step_kind_by_id.get(sid) == "packaging":
                    prev_pack = sid
                    break
            if not prev_pack:
                _fail(f"{workorder_path}: delivery email step {dsid!r} requires a prior packaging step")

            pcfg = step_cfg_by_id.get(prev_pack) or {}
            max_bytes = None
            if isinstance(pcfg.get("packaging"), dict):
                max_bytes = (pcfg.get("packaging") or {}).get("max_bytes")
            if max_bytes is None:
                max_bytes = pcfg.get("max_bytes")
            if max_bytes is None:
                max_bytes = pcfg.get("artifact_max_bytes")

            mb = _as_int(max_bytes)
            if mb is None:
                _fail(
                    f"{workorder_path}: delivery email step {dsid!r} requires packaging step {prev_pack!r} to declare max_bytes < {EMAIL_ATTACHMENT_THRESHOLD_BYTES}"
                )
            if mb >= EMAIL_ATTACHMENT_THRESHOLD_BYTES:
                _fail(
                    f"{workorder_path}: delivery email step {dsid!r} requires packaging step {prev_pack!r} max_bytes < {EMAIL_ATTACHMENT_THRESHOLD_BYTES} (got {mb})"
                )

    # Validate deliverables contract for each module used.
    module_deliverables: Dict[str, Set[str]] = {}
    module_contracts: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for mid in sorted(set(step_module.values())):
        rules = module_rules_by_id.get(mid)
        if not rules:
            _step_fail_or_warn(f"missing module rules for module_id {mid!r} (run Maintenance)")
            # Without rules, we cannot safely validate constraints; skip the rest for drafts.
            continue

        module_kind = _load_module_kind(repo_root, mid)
        contract = _load_module_deliverables_contract(repo_root, mid)
        module_contracts[mid] = contract
        module_deliverables[mid] = set(contract.keys())

        for sid, smid in step_module.items():
            if smid != mid:
                continue
            sk = step_kind_by_id.get(sid)
            if not sk:
                continue
            if sk != module_kind:
                _step_fail_or_warn(f"step {sid!r} kind {sk!r} does not match module {mid!r} kind {module_kind!r}")

        # Validate deliverables.limited_inputs keys against ports rules (must be limited_port).
        _tenant_inputs, all_inputs, _exposed = _index_module_rules(rules)
        for did, dcfg in contract.items():
            lim = (dcfg.get("limited_inputs") or {})
            if not isinstance(lim, dict):
                continue
            for k in lim.keys():
                key = str(k)
                if key not in all_inputs:
                    _step_fail_or_warn(f"module {mid!r} deliverable {did!r} limited_input {key!r} not declared in ports")
                    continue
                rr = all_inputs[key]
                if rr.is_tenant_visible:
                    _step_fail_or_warn(f"module {mid!r} deliverable {did!r} limited_input {key!r} must be limited_port")

    # Validate requested deliverables (explicit + legacy mapping).
    per_step_requested: Dict[str, List[str]] = {}
    per_step_source: Dict[str, str] = {}
    for sid, mid in step_module.items():
        cfg = step_cfg_by_id.get(sid) or {}
        req, src = _normalize_requested_deliverables_for_preflight(repo_root, mid, cfg)
        per_step_requested[sid] = req
        per_step_source[sid] = src
        if req:
            allowed = module_deliverables.get(mid) or set()
            for did in req:
                if did not in allowed:
                    _step_fail_or_warn(f"step {sid!r} deliverable {did!r} not declared by module {mid!r}")

    # Precompute exposed outputs per step (from servicing table) for binding validation.
    step_outputs: Dict[str, Set[str]] = {}
    for sid, mid in step_module.items():
        rules = module_rules_by_id.get(mid) or []
        _, _, exposed = _index_module_rules(rules)
        step_outputs[sid] = exposed

    enriched_steps: List[Dict[str, Any]] = []

    # Validate and enrich step inputs (defaults + deliverables-limited_inputs), then validate bindings.
    for sid, mid in step_module.items():
        rules = module_rules_by_id.get(mid) or []
        tenant_inputs, all_inputs, _ = _index_module_rules(rules)

        cfg = step_cfg_by_id.get(sid) or {}
        inputs_obj = cfg.get("inputs") or {}
        if not isinstance(inputs_obj, dict):
            _step_fail_or_warn(f"step {sid!r} inputs must be an object")
            inputs_obj = {}

        # Reject tenant-provided limited_port inputs and unknown inputs.
        for k in inputs_obj.keys():
            fid = str(k)
            if fid not in all_inputs:
                _step_fail_or_warn(f"step {sid!r} module {mid} has unknown input {fid!r}")
                continue
            rr = all_inputs[fid]
            if not rr.is_tenant_visible:
                _step_fail_or_warn(f"step {sid!r} input {fid!r} is limited_port and must not be set by tenant")

        # Apply defaults (tenant-visible inputs).
        effective: Dict[str, Any] = dict(inputs_obj)
        defaults_applied: List[str] = []
        for fid, rr in all_inputs.items():
            if fid in effective:
                continue
            has_def, dv = _parse_default(rr, f"{workorder_path}: step {sid!r} input {fid!r}")
            if has_def:
                _validate_constraints(rr, dv, f"{workorder_path}: step {sid!r} default {fid!r}")
                effective[fid] = dv
                defaults_applied.append(fid)

        # Apply deliverables-driven platform-only inputs (limited_port)
        req = per_step_requested.get(sid, []) or []
        contract = module_contracts.get(mid) or {}
        applied_limited = _union_limited_inputs(contract, req)
        for k, v in (applied_limited or {}).items():
            if k not in all_inputs:
                _step_fail_or_warn(f"step {sid!r} deliverables set unknown limited_input {k!r} for module {mid!r}")
                continue
            rr = all_inputs[str(k)]
            if rr.is_tenant_visible:
                _step_fail_or_warn(f"step {sid!r} deliverables set tenant-visible input {k!r} (must be limited_port)")
                continue
            _validate_constraints(rr, v, f"{workorder_path}: step {sid!r} deliverables limited_input {k!r}")
            effective[k] = v

        # Required inputs (tenant-visible + limited_port) must be present after enrichment.
        for fid, rr in all_inputs.items():
            if not rr.required:
                continue
            if fid not in effective:
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue
            vv = effective.get(fid)
            if vv is None:
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue
            if isinstance(vv, str) and not vv.strip() and rr.type == "string":
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue

        # Validate each effective input.
        for fid, val in effective.items():
            rr = all_inputs.get(str(fid))
            if rr is None:
                continue
            ctx = f"{workorder_path}: step {sid!r} input {fid!r}"

            # Bindings may appear nested within lists/dicts; validate each binding reference.
            for b in _iter_bindings(val):
                try:
                    _validate_binding(b, rr, ctx, step_ids, step_outputs)
                except ConsistencyValidationError as e:
                    if enabled:
                        raise
                    warnings.append(f"{workorder_path}: draft warning: {str(e)}")

            # If the value itself is a binding object, we skip scalar constraints (bindings are validated above).
            if _is_binding(val):
                continue
            try:
                _validate_constraints(rr, val, ctx)
            except ConsistencyValidationError as e:
                if enabled:
                    raise
                warnings.append(f"{workorder_path}: draft warning: {str(e)}")

        enriched_steps.append(
            {
                "step_id": sid,
                "module_id": mid,
                "kind": step_kind_by_id.get(sid, ""),
                "requested_deliverables": req,
                "deliverables_source": per_step_source.get(sid, "none"),
                "defaults_applied": defaults_applied,
                "effective_inputs": effective,
            }
        )

    return {
        "enabled": enabled,
        "path": str(workorder_path),
        "tenant_id": tid,
        "artifacts_requested": artifacts_requested,
        "steps": enriched_steps,
        "warnings": warnings,
    }


def integrity_validate(repo_root: Path, work_order_id: str = "", tenant_id: str = "", path: str = "") -> List[Dict[str, Any]]:
    # Returns list of per-workorder results. Raises ConsistencyValidationError on failure.
    rules = load_rules_table(repo_root)

    def _resolve_path_from_index(wid: str, tid: str) -> Path:
        idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
        if not idx_path.exists():
            _fail("maintenance-state/workorders_index.csv missing (run Maintenance)")
        rows = read_csv(idx_path)
        matches = []
        for r in rows:
            enabled = str(r.get('enabled','')).strip().lower() == 'true'
            if not enabled:
                continue
            rwid = str(r.get('work_order_id','')).strip()
            rtid = canon_tenant_id(r.get('tenant_id',''))
            rel = str(r.get('path','')).strip()
            if not rel or not rwid:
                continue
            if wid and rwid != wid:
                continue
            if tid and rtid != canon_tenant_id(tid):
                continue
            matches.append(rel)
        if not matches:
            _fail(f"No enabled workorder found for work_order_id={wid!r} tenant_id={tid!r}")
        if len(matches) > 1:
            _fail(f"Multiple enabled workorders match work_order_id={wid!r}; provide --tenant-id or --path")
        return (repo_root / matches[0]).resolve()

    results: List[Dict[str, Any]] = []

    if path:
        wp = Path(path)
        if not wp.is_absolute():
            wp = (repo_root / wp)
        if not wp.exists():
            _fail(f"workorder path not found: {wp}")
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        return results

    if work_order_id:
        wp = _resolve_path_from_index(str(work_order_id), str(tenant_id))
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        return results

    # Validate all enabled workorders from index
    idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
    if not idx_path.exists():
        _fail("maintenance-state/workorders_index.csv missing (run Maintenance)")
    rows = read_csv(idx_path)
    any_validated = False
    for r in rows:
        enabled = str(r.get('enabled','')).strip().lower() == 'true'
        if not enabled:
            continue
        rel = str(r.get('path','')).strip()
        if not rel:
            continue
        wp = repo_root / rel
        if not wp.exists():
            _fail(f"workorders_index references missing file: {rel}")
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        any_validated = True
    if not any_validated:
        return []
    return results


def validate_all_workorders(repo_root: Path) -> None:
    rules = load_rules_table(repo_root)
    idx_path = repo_root / "maintenance-state" / "workorders_index.csv"
    use_scan = (os.environ.get("PLATFORM_DEV_SCAN_WORKORDERS", "").strip() == "1")

    def _emit_warnings(warns: List[str]) -> None:
        for w in warns:
            print(f"DRAFT WARNING: {w}")

    if idx_path.exists() and not use_scan:
        rows = read_csv(idx_path)
        any_seen = False
        for r in rows:
            rel = str(r.get("path", "") or "").strip()
            if not rel:
                continue
            wp = repo_root / rel
            if not wp.exists():
                _fail(f"workorders_index references missing file: {rel}")

            res = validate_workorder_preflight(repo_root, wp, rules)
            any_seen = True
            if not bool(res.get("enabled", False)):
                _emit_warnings(list(res.get("warnings", []) or []))
                continue
        if not any_seen:
            return
        return

    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    any_seen = False
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
            res = validate_workorder_preflight(repo_root, wp, rules)
            any_seen = True
            if not bool(res.get("enabled", False)):
                _emit_warnings(list(res.get("warnings", []) or []))
                continue

    if not any_seen:
        return
        return
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