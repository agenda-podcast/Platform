from __future__ import annotations

"""Orchestrator runner.

The orchestrator implementation is stored in named implementation parts so that all
Python logic files stay at or under 500 lines.

This module does not embed secrets. Secret ingestion remains centralized in the
secretstore loader and workflow environments.
"""

from types import ModuleType
from typing import Any, Dict
import sys

from .._orchestrator_parts.foundations import get_part as _part_foundations
from .._orchestrator_parts.queue_resolution import get_part as _part_queue
from .._orchestrator_parts.pricing_and_billing import get_part as _part_pricing
from .._orchestrator_parts.step_execution import get_part as _part_steps
from .._orchestrator_parts.cache_and_completion import get_part as _part_cache
from .._orchestrator_parts.runtime_evidence import get_part as _part_runtime_evidence
from .._orchestrator_parts.refunds_and_ledger import get_part as _part_refunds


def _load_orchestrator_namespace() -> Dict[str, Any]:
    code = "".join(
        [
            _part_foundations(),
            _part_queue(),
            _part_pricing(),
            _part_steps(),
            _part_cache(),
            _part_runtime_evidence(),
            _part_refunds(),
        ]
    )


    mod_name = "platform.orchestration._orchestrator._impl"

    # Ensure __name__ is set inside the exec() code so dataclasses assigns cls.__module__ reliably
    prefix = "__name__ = '%s'\n__package__ = 'platform.orchestration'\n" % mod_name
    code = prefix + code

    mod = ModuleType(mod_name)
    mod.__dict__['__name__'] = mod_name
    mod.__dict__['__package__'] = 'platform.orchestration'
    mod.__dict__['__file__'] = __file__
    mod.__package__ = "platform.orchestration"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__


_NS = _load_orchestrator_namespace()

run_orchestrator = _NS["run_orchestrator"]

__all__ = ["run_orchestrator"]
