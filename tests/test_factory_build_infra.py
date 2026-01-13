from __future__ import annotations

import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestFactoryBuildInfra(unittest.TestCase):
    def test_build_infra_dev_github(self) -> None:
        repo_root = ensure_repo_on_path()

        from platform.infra.config import load_runtime_profile
        from platform.infra.factory import build_infra

        profile_path = repo_root / "config" / "runtime_profile.dev_github.yml"
        prof = load_runtime_profile(repo_root, cli_path=str(profile_path))

        bundle = build_infra(repo_root=repo_root, profile=prof)
        desc = bundle.describe()

        self.assertEqual(desc.get("profile_name"), "dev_github")
        self.assertIn("adapters", desc)
        self.assertIn("registry", desc["adapters"])
        self.assertIn("class", desc["adapters"]["registry"])


if __name__ == "__main__":
    unittest.main()
