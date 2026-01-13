from __future__ import annotations

from pathlib import Path

from ..utils.hashing import sha256_file as _sha256_file


def sha256_file(path: Path) -> str:
    """Compute SHA-256 for a file.

    A single canonical implementation is exposed here so packaging, publishing,
    and delivery verification can depend on one stable import path.
    """

    return _sha256_file(path)
