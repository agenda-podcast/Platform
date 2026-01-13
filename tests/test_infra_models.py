from __future__ import annotations

import unittest

from _testutil import ensure_repo_on_path


class TestInfraModels(unittest.TestCase):
    def test_dataclasses_instantiate(self) -> None:
        ensure_repo_on_path()

        from platform.infra.models import (
            DeliverableArtifactRecord,
            OutputRecord,
            StepRunRecord,
            StepSpec,
            TransactionItemRecord,
            TransactionRecord,
            WorkorderSpec,
        )

        step = StepSpec(step_id="s1", module_id="m1", inputs={"a": 1}, deliverables=["tenant_outputs"])
        wo = WorkorderSpec(tenant_id="t1", work_order_id="w1", steps=[step], source_path="tenants/t1/workorders/w1.yml")

        _ = StepRunRecord(
            module_run_id="r1",
            tenant_id="t1",
            work_order_id="w1",
            step_id="s1",
            module_id="m1",
            status="COMPLETED",
            created_at="2026-01-01T00:00:00Z",
            metadata={"k": "v"},
            requested_deliverables=["tenant_outputs"],
        )

        _ = OutputRecord(
            tenant_id="t1",
            work_order_id="w1",
            step_id="s1",
            module_id="m1",
            output_id="report",
            path="report.json",
            uri="file:///tmp/report.json",
            sha256="",
            bytes=0,
            bytes_size=0,
            created_at="",
        )

        _ = TransactionRecord(
            transaction_id="tx1",
            tenant_id="t1",
            work_order_id="w1",
            type="SPEND",
            amount_credits=3,
            created_at="2026-01-01T00:00:00Z",
            reason_code="",
            note="",
            metadata_json="{}",
        )

        _ = TransactionItemRecord(
            transaction_item_id="ti1",
            transaction_id="tx1",
            tenant_id="t1",
            module_id="m1",
            work_order_id="w1",
            step_id="s1",
            deliverable_id="__run__",
            feature="run",
            type="SPEND",
            amount_credits=1,
            created_at="2026-01-01T00:00:00Z",
            note="",
            metadata_json="{}",
        )

        _ = DeliverableArtifactRecord(
            tenant_id="t1",
            work_order_id="w1",
            step_id="s1",
            module_id="m1",
            deliverable_id="tenant_outputs",
            artifact_key="releases/t1/w1/s1/tenant_outputs.zip",
            created_at="2026-01-01T00:00:00Z",
        )

        self.assertEqual(wo.steps[0].step_id, "s1")


if __name__ == "__main__":
    unittest.main()
