from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestLocalArtifactStore(unittest.TestCase):
    def test_put_get_exists(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.artifacts_local import LocalArtifactStore

        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / "dist_artifacts"
            store = LocalArtifactStore(base)

            src = Path(td) / "src.txt"
            src.write_text("hello", encoding="utf-8")

            uri = store.put_file("a/b/src.txt", src)
            self.assertTrue(uri.startswith("file://"))
            self.assertTrue(store.exists("a/b/src.txt"))

            dest = Path(td) / "dest.txt"
            store.get_to_file("a/b/src.txt", dest)
            self.assertEqual(dest.read_text(encoding="utf-8"), "hello")


if __name__ == "__main__":
    unittest.main()
