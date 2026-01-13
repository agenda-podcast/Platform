from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


WORKORDER_WITH_DELIVERY_HEADER = """work_order_id: w1
enabled: true
artifacts_requested: false
steps:
  - step_id: s1
    module_id: U2T
    kind: transform
    inputs:
      topic: x
  - step_id: s2
    module_id: package_std
    kind: packaging
    inputs:
      bound_outputs:
"""

WORKORDER_DELIVERY_TAIL = """  - step_id: s3
    module_id: deliver_email
    kind: delivery
    inputs:
      recipient_email: test@example.com
      package_zip:
        from_step: s2
        output_id: package_zip
      manifest_json:
        from_step: s2
        output_id: manifest_json
"""


class TestPackagingBindingSchema(unittest.TestCase):
    def _write_workorder(self, tmp: Path, *, bound_outputs_block: str) -> Path:
        w = tmp / "tenants" / "aaaaaa" / "workorders" / "w1.yml"
        w.parent.mkdir(parents=True, exist_ok=True)
        w.write_text(WORKORDER_WITH_DELIVERY_HEADER + bound_outputs_block + WORKORDER_DELIVERY_TAIL, encoding="utf-8")
        return w

    def _make_repo_copy(self, repo_root: Path, tmp: Path) -> None:
        shutil.copytree(repo_root / "modules", tmp / "modules")
        shutil.copytree(repo_root / "maintenance-state", tmp / "maintenance-state")

    def test_package_std_bound_outputs_requires_output_id(self) -> None:
        ensure_repo_on_path()

        from platform.consistency.validator import ConsistencyValidationError
        from platform.consistency.validator import load_rules_table, validate_workorder_preflight

        repo_root = Path(__file__).resolve().parents[1]
        rules = load_rules_table(repo_root)

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            self._make_repo_copy(repo_root, tmp)

            w = self._write_workorder(
                tmp,
                bound_outputs_block="""        - from_step: s1
          from_file: report
          as_path: reports/report.json
""",
            )

            with self.assertRaises(ConsistencyValidationError) as ctx:
                _ = validate_workorder_preflight(tmp, w, rules)

            msg = str(ctx.exception)
            self.assertIn("binding must include output_id", msg)

    def test_package_std_bound_outputs_missing_output_id_fails(self) -> None:
        ensure_repo_on_path()

        from platform.consistency.validator import ConsistencyValidationError
        from platform.consistency.validator import load_rules_table, validate_workorder_preflight

        repo_root = Path(__file__).resolve().parents[1]
        rules = load_rules_table(repo_root)

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            self._make_repo_copy(repo_root, tmp)

            w = self._write_workorder(
                tmp,
                bound_outputs_block="""        - from_step: s1
          as_path: reports/report.json
""",
            )

            with self.assertRaises(ConsistencyValidationError) as ctx:
                _ = validate_workorder_preflight(tmp, w, rules)

            msg = str(ctx.exception)
            self.assertIn("binding must include output_id", msg)

    def test_package_std_bound_outputs_output_id_ok(self) -> None:
        ensure_repo_on_path()

        from platform.consistency.validator import load_rules_table, validate_workorder_preflight

        repo_root = Path(__file__).resolve().parents[1]
        rules = load_rules_table(repo_root)

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            self._make_repo_copy(repo_root, tmp)

            w = self._write_workorder(
                tmp,
                bound_outputs_block="""        - from_step: s1
          output_id: report
          as_path: reports/report.json
""",
            )

            res = validate_workorder_preflight(tmp, w, rules)
            self.assertTrue(bool(res.get("enabled", True)))


if __name__ == "__main__":
    unittest.main()
