from __future__ import annotations

"""Generated loader for refund safety tests. Do not edit by hand."""

from types import ModuleType
from typing import Any, Dict
from pathlib import Path
import sys

from .._orch_refund_chunks.chunk_01 import get_chunk as _chunk_01
from .._orch_refund_chunks.chunk_02 import get_chunk as _chunk_02


def load_namespace() -> Dict[str, Any]:
    code = "".join([
        _chunk_01(),
        _chunk_02(),
    ])

    mod_name = "tests._orch_refund._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "tests"
    tests_dir = Path(__file__).resolve().parents[1]
    mod_file = tests_dir / "test_orchestrator_delivery_refund_safety.py"
    mod.__file__ = str(mod_file)
    mod.__dict__["__file__"] = mod.__file__
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__
