from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.infra.adapters.runstate_csv` implementation from chunks to keep
all Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._runstate_csv_parts.runstate_reader import get_part as _part_runstate_reader
from .._runstate_csv_parts.runstate_writer import get_part as _part_runstate_writer


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _part_runstate_reader(),
        _part_runstate_writer(),
    ])

    mod_name = "platform.infra.adapters._runstate_csv._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.infra.adapters"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
