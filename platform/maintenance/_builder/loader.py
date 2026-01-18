from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.maintenance.builder` implementation from parts to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._builder_parts.modules_index import get_part as _modules_index
from .._builder_parts.workorders_index import get_part as _workorders_index
from .._builder_parts.prices_and_requirements import get_part as _prices_and_requirements
from .._builder_parts.billing_release_assets import get_part as _billing_release_assets


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _modules_index(),
        _workorders_index(),
        _prices_and_requirements(),
        _billing_release_assets(),
    ])

    mod_name = "platform.maintenance._builder._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.maintenance"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
