from __future__ import annotations

"""Infra factory loader.

Loads `platform.infra.factory` implementation from role-based parts so each
logic file stays at or under 500 lines without mechanical chunk naming.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._factory_parts.bundle_and_models import get_chunk as _bundle_and_models
from .._factory_parts.registry_and_exec import get_chunk as _registry_and_exec


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _bundle_and_models(),
        _registry_and_exec(),
    ])

    mod_name = "platform.infra._factory._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.infra"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
