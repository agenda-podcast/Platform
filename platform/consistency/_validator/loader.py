from __future__ import annotations

"""Role loader for Consistency.

Consistency is responsible for static contract rule parsing only.
Workorder validation lives under platform.orchestration.

This loader composes a small implementation namespace from part files to
keep all Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._validator_parts.rules_table import get_part as _rules_table


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _rules_table(),
    ])

    mod_name = "platform.consistency._validator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.consistency"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
