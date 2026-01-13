from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .checksums import sha256_file


@dataclass(frozen=True)
class ZipEntry:
    arcname: str
    source_path: Path


def create_zip(*, zip_path: Path, entries: Iterable[ZipEntry]) -> Tuple[str, int]:
    """Create a ZIP archive deterministically.

    Returns:
        (sha256, bytes_size)
    """
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for e in entries:
            src = e.source_path
            if not src.exists():
                raise FileNotFoundError(f"ZIP entry source not found: {src}")
            zf.write(src, arcname=e.arcname)

    digest = sha256_file(zip_path)
    size = int(zip_path.stat().st_size)
    return digest, size


def write_manifest_json(*, manifest_path: Path, manifest: Dict[str, Any]) -> None:
    """Write a JSON manifest with stable formatting."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def zip_with_manifest(
    *,
    zip_path: Path,
    entries: List[ZipEntry],
    manifest: Dict[str, Any],
    manifest_arcname: str = "manifest.json",
) -> Tuple[str, int]:
    """Create a ZIP and include a manifest inside the archive."""

    zip_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_manifest = zip_path.parent / (zip_path.name + ".manifest.tmp.json")
    write_manifest_json(manifest_path=tmp_manifest, manifest=manifest)

    try:
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for e in entries:
                src = e.source_path
                if not src.exists():
                    raise FileNotFoundError(f"ZIP entry source not found: {src}")
                zf.write(src, arcname=e.arcname)
            zf.write(tmp_manifest, arcname=manifest_arcname)
    finally:
        try:
            tmp_manifest.unlink(missing_ok=True)
        except Exception:
            pass

    digest = sha256_file(zip_path)
    size = int(zip_path.stat().st_size)
    return digest, size
