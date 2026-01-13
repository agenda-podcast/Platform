from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class FakeGitHubReleaseIO:
    def __init__(self):
        self.upload_calls = []
        self.download_calls = []
        self.assets_by_tag = {}

    def upload(self, *, tag: str, file_paths, overwrite: bool = True) -> None:
        names = [Path(p).name for p in file_paths]
        self.upload_calls.append((tag, names, overwrite))
        cur = self.assets_by_tag.setdefault(tag, set())
        for n in names:
            cur.add(n)

    def list_assets(self, *, tag: str):
        return sorted(list(self.assets_by_tag.get(tag, set())))

    def download(self, *, tag: str, patterns, dest_dir: Path):
        self.download_calls.append((tag, list(patterns)))
        out = []
        for pat in patterns:
            if pat in self.assets_by_tag.get(tag, set()):
                p = dest_dir / pat
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(f"downloaded:{tag}:{pat}", encoding="utf-8")
                out.append(p)
        return out


class TestGitHubReleaseArtifactStore(unittest.TestCase):
    def test_put_exists_get_with_mock_io(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.artifacts_github_release import (
            GitHubReleaseArtifactStore,
            GitHubReleaseArtifactStoreSettings,
        )
        from platform.infra.errors import ValidationError

        fake = FakeGitHubReleaseIO()

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td) / "repo"
            repo_root.mkdir(parents=True, exist_ok=True)

            settings = GitHubReleaseArtifactStoreSettings(default_tag="v1")
            store = GitHubReleaseArtifactStore(repo_root=repo_root, settings=settings, io=fake)

            src = Path(td) / "file.txt"
            src.write_text("hello", encoding="utf-8")

            old = os.environ.pop("GITHUB_TOKEN", None)
            try:
                with self.assertRaises(ValidationError):
                    store.put_file("v1/file.txt", src)
            finally:
                if old is not None:
                    os.environ["GITHUB_TOKEN"] = old

            os.environ["GITHUB_TOKEN"] = "dummy"
            uri = store.put_file("v2/file.txt", src)
            self.assertEqual(uri, "github_release://v2/file.txt")
            self.assertEqual(fake.upload_calls[0][0], "v2")
            self.assertTrue(store.exists("v2/file.txt"))

            dest = Path(td) / "out.txt"
            store.get_to_file("v2/file.txt", dest)
            self.assertTrue(dest.exists())
            self.assertIn("downloaded:v2:file.txt", dest.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
