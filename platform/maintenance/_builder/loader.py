from __future__ import annotations

"""Maintenance builder loader.

Loads `platform.maintenance.builder` implementation from role-based parts so
each logic file stays at or under 500 lines without mechanical chunk naming.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._builder_parts.index_generation import get_chunk as _index_generation
from .._builder_parts.id_and_registry_policy import get_chunk as _id_and_registry_policy
from .._builder_parts.module_requirements_and_prices import get_chunk as _module_requirements_and_prices
from .._builder_parts.billing_release_assets import get_chunk as _billing_release_assets


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _index_generation(),
        _id_and_registry_policy(),
        _module_requirements_and_prices(),
        _billing_release_assets(),
    ])

    mod_name = "platform.maintenance._builder._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.maintenance"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
