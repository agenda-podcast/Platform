from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import yaml

from ..consistency.validator import (
    ConsistencyValidationError,
    _validate_binding,
    _validate_constraints,
)
from ..utils.ids import validate_module_id


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConsistencyValidationError(f"workorder file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise ConsistencyValidationError(f"workorder YAML parse error: {e}")
    return data if isinstance(data, dict) else {}


def _is_binding(v: Any) -> bool:
    if not isinstance(v, dict):
        return False
    step = str(v.get("from_step") or v.get("step_id") or "").strip()
    if not step:
        return False
    has_from_file = bool(str(v.get("from_file") or "").strip())
    has_output_id = bool(str(v.get("output_id") or v.get("from_output_id") or "").strip())
    return has_from_file or has_output_id


def validate_workorder_preflight(
    repo_root: Path,
    workorder_path: Path,
    module_rules_by_id: Dict[str, List[Any]],
) -> None:
    """Deterministic static workorder validation.

    Purpose:
      - Fail fast on invalid enabled workorders before any billing or execution.
      - Enforce module contract rules (required/unknown inputs, constraints, bindings).

    This performs no module execution.
    """

    w = _load_yaml(workorder_path)

    work_order_id = str(w.get("work_order_id") or "").strip()
    if not work_order_id:
        raise ConsistencyValidationError("work_order_id is required")

    steps = w.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ConsistencyValidationError("steps must be a non-empty list")

    used_step_ids: Set[str] = set()
    step_ids: List[str] = []
    step_module: Dict[str, str] = {}

    for i, st in enumerate(steps):
        if not isinstance(st, dict):
            raise ConsistencyValidationError(f"step[{i}] must be an object")
        sid = str(st.get("step_id") or "").strip()
        mid = str(st.get("module_id") or "").strip()
        if not sid:
            raise ConsistencyValidationError(f"step[{i}].step_id is required")
        if sid in used_step_ids:
            raise ConsistencyValidationError(f"duplicate step_id: {sid}")
        used_step_ids.add(sid)
        step_ids.append(sid)

        if not mid:
            raise ConsistencyValidationError(f"step[{i}].module_id is required")
        try:
            validate_module_id(mid)
        except Exception as e:
            raise ConsistencyValidationError(f"invalid module_id {mid!r}: {e}")
        step_module[sid] = mid

        module_dir = repo_root / "modules" / mid
        module_yml = module_dir / "module.yml"
        if not module_yml.exists():
            raise ConsistencyValidationError(f"module.yml not found for module_id={mid} at {module_yml}")

        cfg = st.get("cfg") or {}
        if cfg is None:
            cfg = {}
        if not isinstance(cfg, dict):
            raise ConsistencyValidationError(f"step[{i}].cfg must be an object")
        inputs = cfg.get("inputs") or {}
        if inputs is None:
            inputs = {}
        if not isinstance(inputs, dict):
            raise ConsistencyValidationError(f"step[{i}].cfg.inputs must be an object")

        rules = module_rules_by_id.get(mid, []) or []
        # rules rows are already normalized by load_rules_table; we apply only deterministic checks.
        required: Set[str] = set()
        allowed: Set[str] = set()
        constraints: Dict[str, List[Any]] = {}

        for r in rules:
            if not isinstance(r, dict):
                continue
            io = str(r.get("io") or "").strip().upper()
            scope = str(r.get("scope") or "").strip().lower()
            if io != "INPUT":
                continue
            key = str(r.get("key") or "").strip()
            if not key:
                continue
            # Only tenant-visible inputs are allowed in workorders
            if scope != "port":
                continue
            allowed.add(key)
            if str(r.get("required") or "").strip().lower() == "true":
                required.add(key)
            constraints.setdefault(key, []).append(r)

        missing_required = sorted([k for k in required if k not in inputs])
        if missing_required:
            raise ConsistencyValidationError(
                f"step_id={sid} module_id={mid} missing required inputs: {missing_required}"
            )

        unknown = sorted([k for k in inputs.keys() if k not in allowed])
        if unknown:
            raise ConsistencyValidationError(
                f"step_id={sid} module_id={mid} unknown inputs (not declared as tenant ports): {unknown}"
            )

        # Validate constraints and bindings.
        for k, v in inputs.items():
            if _is_binding(v):
                _validate_binding(
                    module_id=mid,
                    key=k,
                    value=v,
                    step_ids=step_ids,
                    output_ids_by_step={s: set() for s in step_ids},
                )
            else:
                for rr in constraints.get(k, []):
                    _validate_constraints(module_id=mid, key=k, value=v, rule=rr)

        # Ensure binding references only prior steps (simple cycle prevention)
        for k, v in inputs.items():
            if not _is_binding(v):
                continue
            dep = str(v.get("from_step") or v.get("step_id") or "").strip()
            if dep and dep in step_ids:
                if step_ids.index(dep) > step_ids.index(sid):
                    raise ConsistencyValidationError(
                        f"step_id={sid} input={k} binds from future step {dep}"
                    )

    # OK
    return
