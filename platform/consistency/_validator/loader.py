from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.consistency.validator` implementation from chunks to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._validator_chunks.chunk_01 import get_chunk as _chunk_01
from .._validator_chunks.chunk_02 import get_chunk as _chunk_02
from .._validator_chunks.chunk_03 import get_chunk as _chunk_03


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _chunk_01(),
        _chunk_02(),
        _chunk_03(),
    ])

    mod_name = "platform.consistency._validator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.consistency"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
