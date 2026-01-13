from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol

from ..contracts import ArtifactStore
from ..errors import NotFoundError, ValidationError


class GitHubReleaseIO(Protocol):
    def upload(self, *, tag: str, file_paths: List[Path], overwrite: bool = True) -> None:
        raise NotImplementedError

    def list_assets(self, *, tag: str) -> List[str]:
        raise NotImplementedError

    def download(self, *, tag: str, patterns: List[str], dest_dir: Path) -> List[Path]:
        raise NotImplementedError


@dataclass(frozen=True)
class GitHubReleaseArtifactStoreSettings:
    default_tag: str
    token_env_var: str = "GITHUB_TOKEN"


class _DefaultGitHubReleaseIO:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root

    def upload(self, *, tag: str, file_paths: List[Path], overwrite: bool = True) -> None:
        from ...github.releases import upload_release_assets

        upload_release_assets(repo_root=self.repo_root, tag=str(tag), file_paths=[str(p) for p in file_paths], clobber=overwrite)

    def list_assets(self, *, tag: str) -> List[str]:
        from ...github.releases import list_release_assets

        assets = list_release_assets(repo_root=self.repo_root, tag=str(tag))
        return [str(a.get("name", "")) for a in assets if str(a.get("name", "")).strip()]

    def download(self, *, tag: str, patterns: List[str], dest_dir: Path) -> List[Path]:
        from ...github.releases import download_release_assets

        download_release_assets(repo_root=self.repo_root, tag=str(tag), dest_dir=dest_dir, patterns=patterns)
        out: List[Path] = []
        for pat in patterns:
            p = dest_dir / pat
            if p.exists():
                out.append(p)
        return out


class GitHubReleaseArtifactStore(ArtifactStore):
    """ArtifactStore backed by GitHub Releases assets.

    Key format:
      - "<tag>/<asset_name>" uses the explicit tag
      - "<asset_name>" uses the configured default_tag
    """

    def __init__(
        self,
        *,
        repo_root: Path,
        settings: GitHubReleaseArtifactStoreSettings,
        io: Optional[GitHubReleaseIO] = None,
    ):
        self.repo_root = repo_root
        self.settings = settings
        self.io: GitHubReleaseIO = io if io is not None else _DefaultGitHubReleaseIO(repo_root=repo_root)

    def _require_token(self) -> None:
        token = os.environ.get(self.settings.token_env_var, "") or os.environ.get("GH_TOKEN", "")
        if not str(token).strip():
            raise ValidationError(
                f"Missing GitHub token. Set {self.settings.token_env_var} (or GH_TOKEN) to allow uploading release assets."
            )

    def _split_key(self, key: str) -> (str, str):
        k = str(key).lstrip("/")
        if "/" in k:
            tag, name = k.split("/", 1)
            tag = str(tag).strip()
            name = str(name).strip()
        else:
            tag = str(self.settings.default_tag).strip()
            name = k.strip()
        if not tag or not name:
            raise ValidationError(f"Invalid artifact key: {key!r}")
        return tag, name

    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        self._require_token()
        tag, name = self._split_key(key)
        tmp_dir = Path(tempfile.mkdtemp(prefix="gh_release_store_"))
        try:
            staged = tmp_dir / name
            staged.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(local_path), str(staged))
            self.io.upload(tag=tag, file_paths=[staged], overwrite=True)
            return f"github_release://{tag}/{name}"
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

    def exists(self, key: str) -> bool:
        tag, name = self._split_key(key)
        names = self.io.list_assets(tag=tag)
        return name in names

    def get_to_file(self, key: str, dest_path: Path) -> None:
        tag, name = self._split_key(key)
        tmp_dir = Path(tempfile.mkdtemp(prefix="gh_release_download_"))
        try:
            got = self.io.download(tag=tag, patterns=[name], dest_dir=tmp_dir)
            if not got:
                raise NotFoundError(f"Artifact not found in release: {tag}/{name}")
            src = got[0]
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dest_path))
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

    def list_keys(self, prefix: str = "") -> List[str]:
        pref = str(prefix).lstrip("/")
        if not pref:
            tag = str(self.settings.default_tag).strip()
            return sorted([f"{tag}/{n}" for n in self.io.list_assets(tag=tag)])
        if "/" not in pref:
            tag = pref
            return sorted([f"{tag}/{n}" for n in self.io.list_assets(tag=tag)])
        tag, rest = pref.split("/", 1)
        names = [n for n in self.io.list_assets(tag=tag) if str(n).startswith(rest)]
        return sorted([f"{tag}/{n}" for n in names])
