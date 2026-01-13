from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestLocalExecutionBackend(unittest.TestCase):
    def test_execute_module_and_record_outputs(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.exec_local import LocalExecutionBackend
        from platform.infra.adapters.registry_repo import RepoModuleRegistry
        from platform.infra.adapters.runstate_csv import CsvRunStateStore
        from platform.infra.models import StepSpec, WorkorderSpec

        repo_root = Path(__file__).resolve().parents[1]

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            state_dir = td_path / ".billing-state"
            outputs_dir = td_path / "runs" / "s1"

            run_state = CsvRunStateStore(state_dir)
            reg = RepoModuleRegistry(repo_root)
            backend = LocalExecutionBackend(repo_root=repo_root, registry=reg, run_state=run_state)

            wo = WorkorderSpec(tenant_id="t1", work_order_id="w1", steps=[])
            step = StepSpec(step_id="s1", module_id="U2T", inputs={"text": "hello"}, deliverables=["tenant_outputs"])

            step_run, outs, meta = backend.execute_step(repo_root=repo_root, workorder=wo, step=step, outputs_dir=outputs_dir)

            self.assertEqual(step_run.status, "COMPLETED")
            self.assertTrue(len(outs) >= 1)

            logged = run_state.list_outputs("t1", "w1", "s1")
            self.assertTrue(len(logged) >= 1)

            ids = set([o.output_id for o in logged])
            self.assertIn("source_text", ids)
            self.assertIn("report", ids)

            self.assertIn("result", meta)


if __name__ == "__main__":
    unittest.main()
