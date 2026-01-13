from __future__ import annotations

import os
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestRuntimeProfileLoad(unittest.TestCase):
    def test_load_dev_github_profile(self) -> None:
        repo_root = ensure_repo_on_path()

        from platform.infra.config import load_runtime_profile

        # Use CLI path override to avoid depending on default runtime_profile.yml.
        p = repo_root / "config" / "runtime_profile.dev_github.yml"
        self.assertTrue(p.exists())

        # Ensure env overrides do not interfere with this test.
        old_rp = os.environ.pop("PLATFORM_RUNTIME_PROFILE", None)
        old_name = os.environ.pop("PLATFORM_PROFILE_NAME", None)
        try:
            prof = load_runtime_profile(repo_root, cli_path=str(p))
        finally:
            if old_rp is not None:
                os.environ["PLATFORM_RUNTIME_PROFILE"] = old_rp
            if old_name is not None:
                os.environ["PLATFORM_PROFILE_NAME"] = old_name

        self.assertEqual(prof.profile_name, "dev_github")
        self.assertEqual(prof.adapters["registry"].kind, "repo_csv")
        self.assertEqual(prof.adapters["artifact_publisher"].kind, "github_releases")


if __name__ == "__main__":
    unittest.main()
