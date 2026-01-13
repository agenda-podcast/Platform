from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol, Tuple

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


def _split_key(key: str, default_tag: str) -> Tuple[str, str]:
    k = str(key).lstrip("/")
    if not k:
        raise ValidationError("Empty artifact key")
    if "/" in k:
        tag, rest = k.split("/", 1)
        tag = tag.strip()
        rest = rest.strip()
        if not tag or not rest:
            raise ValidationError(f"Invalid artifact key: {key}")
        return tag, rest
    return str(default_tag).strip(), k


class _DefaultGitHubReleaseIO:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root

    def upload(self, *, tag: str, file_paths: List[Path], overwrite: bool = True) -> None:
        from ...github.releases import upload_release_assets

        upload_release_assets(tag=str(tag), files=file_paths, clobber=overwrite, repo_root=self.repo_root)

    def list_assets(self, *, tag: str) -> List[str]:
        from ...github.releases import list_release_assets

        assets = list_release_assets(tag=str(tag), repo_root=self.repo_root)
        out: List[str] = []
        for a in assets:
            name = str(a.get("name", "")).strip()
            if name:
                out.append(name)
        return out

    def download(self, *, tag: str, patterns: List[str], dest_dir: Path) -> List[Path]:
        from ...github.releases import download_release_assets

        download_release_assets(tag=str(tag), dest_dir=dest_dir, patterns=patterns, repo_root=self.repo_root, clobber=True)
        out: List[Path] = []
        for pat in patterns:
            p = dest_dir / pat
            if p.exists():
                out.append(p)
        return out


class GitHubReleaseArtifactStore(ArtifactStore):
    """ArtifactStore backed by GitHub Release assets.

    Key formats:
      - "<tag>/<asset_name>" uses the explicit tag
      - "<asset_name>" uses settings.default_tag
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
                f"Missing GitHub token in env var {self.settings.token_env_var} (or GH_TOKEN). "
                "Required for GitHub Releases artifact publishing."
            )

    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        self._require_token()
        tag, asset_name = _split_key(key, self.settings.default_tag)

        lp = Path(local_path)
        if not lp.exists() or not lp.is_file():
            raise NotFoundError(f"Local file not found: {local_path}")

        tmp_dir = Path(tempfile.mkdtemp(prefix="artifact_ghrel_"))
        try:
            staged = tmp_dir / asset_name
            staged.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(str(lp), str(staged))

            self.io.upload(tag=tag, file_paths=[staged], overwrite=True)
            return f"github_release:{tag}/{asset_name}"
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

    def get_to_file(self, key: str, dest_path: Path) -> None:
        self._require_token()
        tag, asset_name = _split_key(key, self.settings.default_tag)

        dp = Path(dest_path)
        dp.parent.mkdir(parents=True, exist_ok=True)

        tmp_dir = Path(tempfile.mkdtemp(prefix="artifact_ghrel_get_"))
        try:
            found = self.io.download(tag=tag, patterns=[asset_name], dest_dir=tmp_dir)
            if not found:
                raise NotFoundError(f"Artifact not found: {key}")
            shutil.copyfile(str(found[0]), str(dp))
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=True)

    def exists(self, key: str) -> bool:
        tag, asset_name = _split_key(key, self.settings.default_tag)
        try:
            names = self.io.list_assets(tag=tag)
        except Exception:
            return False
        return asset_name in set(names)

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
