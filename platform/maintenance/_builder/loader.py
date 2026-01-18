from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.maintenance.builder` implementation from chunks to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._builder_parts.modules_registry import get_part as _part_modules_registry
from .._builder_parts.workorders_registry import get_part as _part_workorders_registry
from .._builder_parts.requirements_and_ports import get_part as _part_requirements_and_ports
from .._builder_parts.reasons_and_indexes import get_part as _part_reasons_and_indexes


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _part_modules_registry(),
        _part_workorders_registry(),
        _part_requirements_and_ports(),
        _part_reasons_and_indexes(),
    ])

    mod_name = "platform.maintenance._builder._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.maintenance"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
