from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.maintenance.builder` implementation from chunks to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._builder_chunks.chunk_01 import get_chunk as _chunk_01
from .._builder_chunks.chunk_02 import get_chunk as _chunk_02
from .._builder_chunks.chunk_03 import get_chunk as _chunk_03
from .._builder_chunks.chunk_04 import get_chunk as _chunk_04


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _chunk_01(),
        _chunk_02(),
        _chunk_03(),
        _chunk_04(),
    ])

    mod_name = "platform.maintenance._builder._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.maintenance"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
