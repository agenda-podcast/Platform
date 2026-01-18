from __future__ import annotations

"""Generated loader. Do not edit by hand.

Loads `platform.infra.factory` implementation from chunks to keep all
Python logic files <= 500 lines.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._factory_chunks.chunk_01 import get_chunk as _chunk_01
from .._factory_chunks.chunk_02 import get_chunk as _chunk_02


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _chunk_01(),
        _chunk_02(),
    ])

    mod_name = "platform.infra._factory._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.infra"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
