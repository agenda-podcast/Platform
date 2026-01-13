from __future__ import annotations

import hashlib


def _hash(parts: list[str]) -> str:
    msg = "|".join([str(x) for x in parts])
    return hashlib.sha256(msg.encode("utf-8")).hexdigest()[:24]


def key_workorder_spend(*, tenant_id: str, work_order_id: str, workorder_path: str, plan_type: str) -> str:
    return "wo_spend_" + _hash([tenant_id, work_order_id, workorder_path, plan_type])


def key_step_run(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str) -> str:
    return "step_run_" + _hash([tenant_id, work_order_id, step_id, module_id])


def key_step_run_charge(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str) -> str:
    return "ti_spend_run_" + _hash([tenant_id, work_order_id, step_id, module_id, "__run__"]) 


def key_deliverable_charge(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str, deliverable_id: str) -> str:
    return "ti_spend_deliv_" + _hash([tenant_id, work_order_id, step_id, module_id, deliverable_id])


def key_refund(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str, deliverable_id: str, reason_key: str) -> str:
    return "ti_refund_" + _hash([tenant_id, work_order_id, step_id, module_id, deliverable_id, reason_key])


def key_delivery_evidence(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str) -> str:
    """Idempotency key for a delivery evidence transaction item.

    This is a zero-credit audit line item that records provider evidence (remote path, verification, bytes).
    It must not duplicate on workorder reruns.
    """
    return "ti_delivery_evidence_" + _hash([tenant_id, work_order_id, step_id, module_id, "delivery_evidence"])


def key_artifact_publish(*, tenant_id: str, work_order_id: str, step_id: str, module_id: str, deliverable_id: str, artifact_key: str) -> str:
    return "artifact_publish_" + _hash([tenant_id, work_order_id, step_id, module_id, deliverable_id, artifact_key])
