from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestWorkorderKindValidation(unittest.TestCase):
    def test_enabled_workorder_missing_step_kind_blocks(self) -> None:
        ensure_repo_on_path()


        repo_root = Path(__file__).resolve().parents[1]
        rules = load_rules_table(repo_root)

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            shutil.copytree(repo_root / "modules", tmp / "modules")
            shutil.copytree(repo_root / "maintenance-state", tmp / "maintenance-state")
            (tmp / "tenants" / "aaaaaa" / "workorders").mkdir(parents=True, exist_ok=True)

            # enabled workorder, missing step.kind
            w = tmp / "tenants" / "aaaaaa" / "workorders" / "w1.yml"
            w.write_text(
                """work_order_id: w1
enabled: true
artifacts_requested: false
steps:
  - step_id: s1
    module_id: U2T
    inputs: { }
""",
                encoding="utf-8",
            )

            with self.assertRaises(ConsistencyValidationError):
                _ = validate_workorder_preflight(tmp, w, rules)

    def test_disabled_workorder_missing_kind_is_warning(self) -> None:
        ensure_repo_on_path()


        repo_root = Path(__file__).resolve().parents[1]
        rules = load_rules_table(repo_root)

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            shutil.copytree(repo_root / "modules", tmp / "modules")
            shutil.copytree(repo_root / "maintenance-state", tmp / "maintenance-state")
            (tmp / "tenants" / "aaaaaa" / "workorders").mkdir(parents=True, exist_ok=True)

            w = tmp / "tenants" / "aaaaaa" / "workorders" / "w1.yml"
            w.write_text(
                """work_order_id: w1
enabled: false
steps:
  - step_id: s1
    module_id: U2T
    inputs: { }
""",
                encoding="utf-8",
            )

            res = validate_workorder_preflight(tmp, w, rules)
            self.assertFalse(bool(res.get("enabled", True)))
            warns = list(res.get("warnings", []) or [])
            self.assertTrue(any("missing required field 'kind'" in x for x in warns))


if __name__ == "__main__":
    unittest.main()
