from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestRepoModuleRegistry(unittest.TestCase):
    def test_contract_and_validate_workorder(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.registry_repo import RepoModuleRegistry

        repo_root = Path(__file__).resolve().parents[1]
        reg = RepoModuleRegistry(repo_root)

        mods = reg.list_modules()
        self.assertIn("U2T", mods)

        contract = reg.get_contract("U2T")
        self.assertEqual(contract["module_id"], "U2T")
        self.assertIn("outputs", contract)
        self.assertIn("kind", contract)
        self.assertEqual(contract["kind"], "transform")

        deliverables = reg.list_deliverables("U2T")
        self.assertIn("tenant_outputs", deliverables)

        d = reg.get_deliverable("U2T", "tenant_outputs")
        self.assertEqual(d["deliverable_id"], "tenant_outputs")

        workorder_path = repo_root / "tenants" / "nxlkGI" / "workorders" / "UbjkpxZO.yml"
        reg.validate_workorder(repo_root=repo_root, workorder_path=workorder_path)

    def test_missing_kind_rejected(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.registry_repo import RepoModuleRegistry
        from platform.infra.errors import ValidationError

        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            (tmp / "modules").mkdir(parents=True, exist_ok=True)
            shutil.copytree(repo_root / "modules" / "U2T", tmp / "modules" / "U2T")

            # Remove kind from module.yml
            yml_path = tmp / "modules" / "U2T" / "module.yml"
            txt = yml_path.read_text(encoding="utf-8")
            txt = "\n".join([ln for ln in txt.splitlines() if not ln.strip().startswith("kind:")]) + "\n"
            yml_path.write_text(txt, encoding="utf-8")

            reg = RepoModuleRegistry(tmp)
            with self.assertRaises(ValidationError):
                _ = reg.get_contract("U2T")


if __name__ == "__main__":
    unittest.main()
