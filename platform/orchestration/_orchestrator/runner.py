from __future__ import annotations

"""Orchestrator runner.

The orchestrator implementation is stored in generated chunk files so that all
Python logic files stay at or under 500 lines.

This module does not embed secrets. Secret ingestion remains centralized in the
secretstore loader and workflow environments.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._orch_chunks.chunk_01 import get_chunk as _chunk_01
from .._orch_chunks.chunk_02 import get_chunk as _chunk_02
from .._orch_chunks.chunk_03 import get_chunk as _chunk_03
from .._orch_chunks.chunk_04 import get_chunk as _chunk_04
from .._orch_chunks.chunk_05 import get_chunk as _chunk_05
from .._orch_chunks.chunk_06 import get_chunk as _chunk_06


def _load_orchestrator_namespace() -> Dict[str, Any]:
    code = "".join(
        [
            _chunk_01(),
            _chunk_02(),
            _chunk_03(),
            _chunk_04(),
            _chunk_05(),
            _chunk_06(),
        ]
    )

    mod_name = "platform.orchestration._orchestrator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.orchestration"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__


_NS = _load_orchestrator_namespace()

run_orchestrator = _NS["run_orchestrator"]

__all__ = ["run_orchestrator"]
