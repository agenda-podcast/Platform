from __future__ import annotations

"""Loads `platform.consistency.validator` implementation from role-oriented parts.

This keeps individual Python logic files at or under 500 lines while preserving
existing public API surface.

We execute the generated implementation inside a real module registered in
`sys.modules` so that `@dataclass` and other runtime reflection behave
deterministically.
"""

from types import ModuleType
from typing import Any, Dict
import sys


def load_namespace() -> Dict[str, Any]:
    from .._validator_parts.rules_table import get_part as _rules_table
    from .._validator_parts.workorder_validation import get_part as _workorder_validation
    from .._validator_parts.integrity_checks import get_part as _integrity_checks

    code = "".join([
        _rules_table(),
        _workorder_validation(),
        _integrity_checks(),
    ])

    mod_name = "platform.consistency._validator_impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.consistency"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
