from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestArtifactsChecksums(unittest.TestCase):
    def test_sha256_file_canonical_import(self) -> None:
        ensure_repo_on_path()

        from platform.artifacts.checksums import sha256_file

        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "a.txt"
            p.write_text("hello", encoding="utf-8")
            d1 = sha256_file(p)
            d2 = sha256_file(p)
            self.assertEqual(d1, d2)
            self.assertEqual(len(d1), 64)


if __name__ == "__main__":
    unittest.main()
