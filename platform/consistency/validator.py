from __future__ import annotations

"""Consistency: static contract rule parsing.

Role separation:
- Consistency owns parsing and validating the static contract rules table produced
  by Maintenance.
- Orchestrator owns workorder validation and gating.

This module loads a small implementation namespace from part files to keep
Python logic files within the repository line limits.

Public API:
- ConsistencyValidationError
- RuleRow
- load_rules_table(repo_root)

It also exposes several internal helper functions (prefixed underscore) that are
used by Orchestrator workorder validation. These helpers are treated as stable
within this repository.
"""

from typing import Any, Dict, List

from ..common.id_codec import canon_module_id
from ..utils.csvio import read_csv

from ._validator.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()


def load_rules_table(repo_root) -> Dict[str, List[Any]]:
    """Load maintenance-state/module_contract_rules.csv into a module_id index."""

    path = repo_root / "maintenance-state" / "module_contract_rules.csv"
    if not path.exists():
        _fail(f"Missing servicing table: {path}")

    rows = read_csv(path)
    out: Dict[str, List[Any]] = {}

    RuleRow = _NS.get("RuleRow")
    if RuleRow is None:
        raise RuntimeError("Consistency RuleRow is not available")

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


def __getattr__(name: str):
    if name in _NS:
        return _NS[name]
    raise AttributeError(name)


def __dir__():
    return sorted(set(list(globals().keys()) + list(_NS.keys())))


# Explicitly re-export selected names for clarity.
ConsistencyValidationError = _NS["ConsistencyValidationError"]
RuleRow = _NS["RuleRow"]
_fail = _NS["_fail"]
_is_binding = _NS["_is_binding"]
_iter_bindings = _NS["_iter_bindings"]
_validate_binding = _NS["_validate_binding"]
_validate_constraints = _NS["_validate_constraints"]

__all__ = [
    "ConsistencyValidationError",
    "RuleRow",
    "load_rules_table",
    "_fail",
    "_is_binding",
    "_iter_bindings",
    "_validate_binding",
    "_validate_constraints",
]
