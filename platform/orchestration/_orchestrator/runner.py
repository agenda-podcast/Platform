from __future__ import annotations

from types import ModuleType
from typing import Any, Dict
import sys

from .._engine_parts.policy_and_secrets import get_part as _part_policy_and_secrets
from .._engine_parts.workorder_discovery import get_part as _part_workorder_discovery
from .._engine_parts.bindings_and_runner import get_part as _part_bindings_and_runner
from .._engine_parts.billing_and_refunds import get_part as _part_billing_and_refunds
from .._engine_parts.cache_and_release import get_part as _part_cache_and_release
from .._engine_parts.maintenance_helpers import get_part as _part_maintenance_helpers


def _load_orchestrator_namespace() -> Dict[str, Any]:
    code = "".join([
        _part_policy_and_secrets(),
        _part_workorder_discovery(),
        _part_bindings_and_runner(),
        _part_billing_and_refunds(),
        _part_cache_and_release(),
        _part_maintenance_helpers(),
    ])

    mod_name = "platform.orchestration._orchestrator._impl"
    mod = ModuleType(mod_name)
    mod.__package__ = "platform.orchestration"
    sys.modules[mod_name] = mod

    exec(code, mod.__dict__, mod.__dict__)
    return mod.__dict__


_NS = _load_orchestrator_namespace()

run_orchestrator = _NS["run_orchestrator"]

__all__ = ["run_orchestrator"]
