from __future__ import annotations

import csv
import shutil
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestCacheIndexSmoke(unittest.TestCase):
    def test_cache_index_seed_append_save_reload(self) -> None:
        repo_root = ensure_repo_on_path()

        from platform.billing.state import BillingState

        seed_dir = repo_root / "billing-state-seed"
        self.assertTrue(seed_dir.exists())

        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            shutil.copytree(seed_dir, state_dir)

            billing = BillingState(state_dir)
            rows = billing.load_table("cache_index.csv")

            expected_headers = ["place", "type", "ref", "created_at", "expires_at"]
            with (state_dir / "cache_index.csv").open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                self.assertEqual(reader.fieldnames, expected_headers)

            before = len(rows)
            rows.append(
                {
                    "place": "cache",
                    "type": "module_run",
                    "ref": "cachekey_test_0001",
                    "created_at": "2026-01-01T00:00:00Z",
                    "expires_at": "2026-01-02T00:00:00Z",
                }
            )
            billing.save_table("cache_index.csv", rows, headers=expected_headers)

            reloaded = billing.load_table("cache_index.csv")
            self.assertEqual(len(reloaded), before + 1)

            with (state_dir / "cache_index.csv").open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                self.assertEqual(reader.fieldnames, expected_headers)


if __name__ == "__main__":
    unittest.main()
