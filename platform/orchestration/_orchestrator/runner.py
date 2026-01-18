from __future__ import annotations

"""Orchestrator runner.

Implementation is assembled from role-based parts to keep each logic file at or
under 500 lines while avoiding mechanical "chunk" naming.

Secrets are never embedded here. Secret ingestion remains centralized in the
secretstore loader and workflow environments.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._orchestrator_parts.foundations import get_chunk as _foundations
from .._orchestrator_parts.registries_and_pricing import get_chunk as _registries_and_pricing
from .._orchestrator_parts.run_setup import get_chunk as _run_setup
from .._orchestrator_parts.billing_gate_and_spend import get_chunk as _billing_gate_and_spend
from .._orchestrator_parts.step_execution_and_artifacts import get_chunk as _step_execution_and_artifacts
from .._orchestrator_parts.refunds_and_finalize import get_chunk as _refunds_and_finalize


def _load_orchestrator_namespace() -> Dict[str, Any]:
    code = "".join(
        [
            _foundations(),
            _registries_and_pricing(),
            _run_setup(),
            _billing_gate_and_spend(),
            _step_execution_and_artifacts(),
            _refunds_and_finalize(),
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
