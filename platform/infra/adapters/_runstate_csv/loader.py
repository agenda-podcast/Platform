from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.infra.adapters.runstate_csv` implementation from parts to keep
all Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._runstate_csv_parts.runstate_read_write import get_part as _runstate_read_write
from .._runstate_csv_parts.evidence_and_pricing import get_part as _evidence_and_pricing


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _runstate_read_write(),
        _evidence_and_pricing(),
    ])

    mod_name = "platform.infra.adapters._runstate_csv._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.infra.adapters"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
