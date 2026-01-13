from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Set


@dataclass(frozen=True)
class StatusInputs:
    step_statuses: Dict[str, str]  # step_id -> status
    refunds_exist: bool
    publish_required: bool
    publish_completed: bool


def reduce_workorder_status(inputs: StatusInputs) -> str:
    """Compute canonical workorder status.

    Canonical outputs:
      - COMPLETED: all steps completed, publish gating satisfied, no refunds
      - PARTIAL: refunds exist OR publish gating incomplete with completed steps
      - FAILED: at least one step failed and mode requires fail-fast
      - AWAITING_PUBLISH: steps completed but publish gating pending
      - RUNNING: any step running
      - CREATED: no steps started
    """

    statuses = [str(s or "").upper() for s in inputs.step_statuses.values()]
    if not statuses:
        return "CREATED"

    if any(s in ("RUNNING", "STARTED") for s in statuses):
        return "RUNNING"

    any_failed = any(s in ("FAILED", "ERROR") for s in statuses)
    all_completed = all(s in ("COMPLETED", "SUCCEEDED") for s in statuses)

    if inputs.publish_required and all_completed and not inputs.publish_completed:
        return "AWAITING_PUBLISH"

    if all_completed:
        if inputs.refunds_exist or (inputs.publish_required and not inputs.publish_completed):
            return "PARTIAL"
        return "COMPLETED"

    if any_failed:
        return "FAILED"

    return "PARTIAL"
