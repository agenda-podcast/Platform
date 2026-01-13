from __future__ import annotations

import shutil
from pathlib import Path
from typing import List

from ..contracts import ArtifactStore
from ..errors import NotFoundError


class LocalArtifactStore(ArtifactStore):
    """ArtifactStore for local filesystem.

    Keys are treated as relative paths under base_dir.
    Returned URIs are file:// absolute URIs.
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def _resolve_key(self, key: str) -> Path:
        rel = Path(str(key).lstrip("/")).as_posix()
        return (self.base_dir / rel).resolve()

    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        dest = self._resolve_key(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(local_path), str(dest))
        return dest.as_uri()

    def get_to_file(self, key: str, dest_path: Path) -> None:
        src = self._resolve_key(key)
        if not src.exists():
            raise NotFoundError(f"Artifact not found: {key}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dest_path))

    def exists(self, key: str) -> bool:
        return self._resolve_key(key).exists()

    def list_keys(self, prefix: str = "") -> List[str]:
        base = self.base_dir
        if not base.exists():
            return []
        pref = str(prefix or "").lstrip("/")
        out: List[str] = []
        for p in base.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(base).as_posix()
            if pref and not rel.startswith(pref):
                continue
            out.append(rel)
        return sorted(out)
