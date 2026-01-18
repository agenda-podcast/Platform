from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.infra.factory` implementation from chunks to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._factory_parts.bundle_and_models import get_part as _part_bundle_and_models
from .._factory_parts.registry_and_exec import get_part as _part_registry_and_exec


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _part_bundle_and_models(),
        _part_registry_and_exec(),
    ])

    mod_name = "platform.infra._factory._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.infra"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
