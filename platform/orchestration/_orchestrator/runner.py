from __future__ import annotations

"""Orchestrator runner.

Executes the orchestrator implementation stored in role-based parts.

Implementation note:
- Each part is compiled with a stable filename before execution. This ensures
  tracebacks do not show "<string>" and line numbers remain bounded by each
  part's own file size (<=500 lines).

This module does not embed secrets.
"""

from types import ModuleType
from typing import Any, Dict, List, Tuple
import sys

from .._orchestrator_parts.foundations import get_part as _part_foundations
from .._orchestrator_parts.queue_resolution import get_part as _part_queue
from .._orchestrator_parts.pricing_and_billing import get_part as _part_pricing
from .._orchestrator_parts.step_execution import get_part as _part_steps
from .._orchestrator_parts.cache_and_completion import get_part as _part_cache
from .._orchestrator_parts.refunds_and_ledger import get_part as _part_refunds


def _iter_parts() -> List[Tuple[str, str]]:
    return [
        ("platform/orchestration/_orchestrator_parts/foundations.py", _part_foundations()),
        ("platform/orchestration/_orchestrator_parts/queue_resolution.py", _part_queue()),
        ("platform/orchestration/_orchestrator_parts/pricing_and_billing.py", _part_pricing()),
        ("platform/orchestration/_orchestrator_parts/step_execution.py", _part_steps()),
        ("platform/orchestration/_orchestrator_parts/cache_and_completion.py", _part_cache()),
        ("platform/orchestration/_orchestrator_parts/refunds_and_ledger.py", _part_refunds()),
    ]


def _load_orchestrator_namespace() -> Dict[str, Any]:
    mod_name = "platform.orchestration._orchestrator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.orchestration"
    sys.modules[mod_name] = mod

    for filename, code in _iter_parts():
        compiled = compile(code, filename, "exec")
        exec(compiled, mod.__dict__, mod.__dict__)

    return mod.__dict__


_NS = _load_orchestrator_namespace()

run_orchestrator = _NS["run_orchestrator"]

__all__ = ["run_orchestrator"]
