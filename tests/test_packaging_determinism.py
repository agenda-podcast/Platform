from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from _testutil import ensure_repo_on_path


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class TestPackagingDeterminism(unittest.TestCase):
    def test_package_std_is_deterministic_across_input_order(self) -> None:
        ensure_repo_on_path()

        from modules.package_std.src.run import run as package_run

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            src = tmp / "src"
            src.mkdir(parents=True)

            a = src / "a.txt"
            b = src / "b.txt"
            a.write_text("alpha\n", encoding="utf-8")
            b.write_text("bravo\n", encoding="utf-8")

            rec_a = {
                "step_id": "s1",
                "module_id": "U2T",
                "output_id": "a",
                "uri": a.resolve().as_uri(),
                "as_path": "texts/a.txt",
                "content_type": "text/plain",
            }
            rec_b = {
                "step_id": "s1",
                "module_id": "U2T",
                "output_id": "b",
                "uri": b.resolve().as_uri(),
                "as_path": "texts/b.txt",
                "content_type": "text/plain",
            }

            out1 = tmp / "out1"
            out2 = tmp / "out2"

            res1 = package_run(params={"inputs": {"bound_outputs": [rec_a, rec_b]}}, outputs_dir=out1)
            res2 = package_run(params={"inputs": {"bound_outputs": [rec_b, rec_a]}}, outputs_dir=out2)

            self.assertEqual(res1.get("status"), "COMPLETED")
            self.assertEqual(res2.get("status"), "COMPLETED")

            m1 = (out1 / "manifest.json").read_bytes()
            m2 = (out2 / "manifest.json").read_bytes()
            self.assertEqual(_sha256_bytes(m1), _sha256_bytes(m2))

            z1 = out1 / "package.zip"
            z2 = out2 / "package.zip"
            self.assertEqual(_sha256_file(z1), _sha256_file(z2))

            # ZIP entry order is deterministic and sorted.
            with zipfile.ZipFile(z1, "r") as zf:
                names = zf.namelist()
            self.assertEqual(names, sorted(names))
            self.assertEqual(names[0], "manifest.csv")
            self.assertEqual(names[1], "manifest.json")

            # Manifest rows are stable, sorted by dest_path.
            jj = json.loads((out1 / "manifest.json").read_text(encoding="utf-8"))
            files = jj.get("files") or []
            dests = [r.get("dest_path") for r in files]
            self.assertEqual(dests, sorted(dests))


if __name__ == "__main__":
    unittest.main()
