from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestCsvRunStateStore(unittest.TestCase):
    def test_csv_roundtrip_and_idempotency(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.runstate_csv import CsvRunStateStore, MODULE_RUNS_LOG_HEADERS
        from platform.infra.models import OutputRecord

        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td)
            store = CsvRunStateStore(state_dir)

            run_id = store.create_run(tenant_id="t1", work_order_id="w1", metadata={"a": 1})
            self.assertEqual(run_id, "w1")

            step_run_1 = store.create_step_run(
                tenant_id="t1",
                work_order_id="w1",
                step_id="s1",
                module_id="m1",
                idempotency_key="k1",
                outputs_dir=state_dir / "runs" / "s1",
            )

            step_run_2 = store.create_step_run(
                tenant_id="t1",
                work_order_id="w1",
                step_id="s1",
                module_id="m1",
                idempotency_key="k1",
                outputs_dir=state_dir / "runs" / "s1",
            )

            self.assertEqual(step_run_1.module_run_id, step_run_2.module_run_id)

            module_runs_path = state_dir / "module_runs_log.csv"
            with module_runs_path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            created_rows = [r for r in rows if r.get("status") == "CREATED"]
            self.assertEqual(len(created_rows), 1)
            self.assertEqual(reader.fieldnames, MODULE_RUNS_LOG_HEADERS)

            # Outputs log headers include new metadata fields (backward compatible for older rows).
            outputs_path = state_dir / "outputs_log.csv"
            with outputs_path.open("r", encoding="utf-8", newline="") as f2:
                out_reader = csv.DictReader(f2)
                self.assertIn("content_type", out_reader.fieldnames or [])
                self.assertIn("bytes", out_reader.fieldnames or [])

            store.mark_step_run_running(step_run_1.module_run_id)
            store.mark_step_run_succeeded(step_run_1.module_run_id, requested_deliverables=["tenant_outputs"], metadata={"x": 1})

            out_rec = OutputRecord(
                tenant_id="t1",
                work_order_id="w1",
                step_id="s1",
                module_id="m1",
                output_id="o1",
                path="outputs/o1.json",
                uri=(state_dir / "runs" / "s1" / "outputs" / "o1.json").resolve().as_uri(),
                sha256="",
                bytes=0,
                bytes_size=0,
                created_at="",
            )
            store.record_output(out_rec)

            outs = store.list_outputs("t1", "w1", "s1")
            self.assertEqual(len(outs), 1)
            self.assertEqual(outs[0].output_id, "o1")

            got = store.get_output("t1", "w1", "s1", "o1")
            self.assertEqual(got.path, "outputs/o1.json")


            # Published artifacts log: append-only with idempotency "latest wins"
            from platform.infra.models import DeliverableArtifactRecord

            a1 = DeliverableArtifactRecord(
                tenant_id="t1",
                work_order_id="w1",
                step_id="s1",
                module_id="m1",
                deliverable_id="tenant_outputs",
                artifact_key="k/one.zip",
                artifact_uri="file:///one.zip",
                sha256="abc",
                bytes=1,
                bytes_size=1,
                status="PUBLISHED",
                created_at="2020-01-01T00:00:00Z",
                idempotency_key="pub1",
                metadata={"content_type": "application/zip"},
            )
            a2 = DeliverableArtifactRecord(
                tenant_id="t1",
                work_order_id="w1",
                step_id="s1",
                module_id="m1",
                deliverable_id="tenant_outputs",
                artifact_key="k/two.zip",
                artifact_uri="file:///two.zip",
                sha256="def",
                bytes=2,
                bytes_size=2,
                status="PUBLISHED",
                created_at="2020-01-02T00:00:00Z",
                idempotency_key="pub1",
                metadata={"content_type": "application/zip"},
            )
            store.record_deliverable_artifact(a1)
            store.record_deliverable_artifact(a2)

            pubs = store.list_published_artifacts(tenant_id="t1", work_order_id="w1")
            # latest wins by idempotency_key
            self.assertEqual(len(pubs), 1)
            self.assertEqual(pubs[0].artifact_key, "k/two.zip")
            self.assertEqual((state_dir / "published_artifacts_log.csv").exists(), True)


if __name__ == "__main__":
    unittest.main()
