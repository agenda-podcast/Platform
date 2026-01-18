# Generated. Do not edit by hand.
CHUNK = r'''\
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

'''

def get_chunk() -> str:
    return CHUNK
