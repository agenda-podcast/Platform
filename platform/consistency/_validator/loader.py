from __future__ import annotations

"""Validator loader.

Loads `platform.consistency.validator` implementation from role-based parts so
each logic file stays at or under 500 lines without mechanical chunk naming.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._validator_parts.rules_table import get_chunk as _rules_table
from .._validator_parts.workorder_preflight import get_chunk as _workorder_preflight
from .._validator_parts.integrity_checks import get_chunk as _integrity_checks


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _rules_table(),
        _workorder_preflight(),
        _integrity_checks(),
    ])

    mod_name = "platform.consistency._validator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.consistency"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
