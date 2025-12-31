from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Iterable, List


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def release_exists(tag: str) -> bool:
    cp = _run(["gh", "release", "view", tag])
    return cp.returncode == 0


def ensure_release(tag: str, title: str) -> None:
    """Ensure a GitHub Release exists. No-op if already present."""
    if release_exists(tag):
        return
    cp = _run(["gh", "release", "create", tag, "--title", title, "--notes", ""]) 
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to create release {tag}: {cp.stderr.strip()}")


def download_release_assets(tag: str, dest_dir: Path, patterns: Iterable[str]) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["gh", "release", "download", tag, "--dir", str(dest_dir)]
    for p in patterns:
        cmd.extend(["--pattern", p])
    cp = _run(cmd)
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to download release assets for {tag}: {cp.stderr.strip()}")


def upload_release_assets(tag: str, files: Iterable[Path], clobber: bool = True) -> None:
    cmd = ["gh", "release", "upload", tag]
    if clobber:
        cmd.append("--clobber")
    cmd.extend([str(p) for p in files])
    cp = _run(cmd)
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to upload release assets for {tag}: {cp.stderr.strip()}")
