from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def release_exists(tag: str) -> bool:
    cp = _run(["gh", "release", "view", tag])
    return cp.returncode == 0


def ensure_release(tag: str, title: str, notes: str = "") -> None:
    if release_exists(tag):
        return
    cp = _run(["gh", "release", "create", tag, "--title", title, "--notes", notes])
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to create release {tag}: {cp.stderr.strip()}")


def download_release_assets(tag: str, dest_dir: Path, patterns: Optional[List[str]] = None) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["gh", "release", "download", tag, "--dir", str(dest_dir)]
    if patterns:
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


def _release_view_json(tag: str) -> Dict[str, object]:
    cp = _run(["gh", "release", "view", tag, "--json", "id,tagName,assets"])
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to view release {tag}: {cp.stderr.strip()}")
    try:
        return json.loads(cp.stdout or "{}")
    except Exception as e:
        raise RuntimeError(f"Failed to parse release view JSON for {tag}") from e


def get_release_numeric_id(tag: str) -> int:
    obj = _release_view_json(tag)
    rid = obj.get("id")
    if rid is None:
        raise RuntimeError(f"Release id missing for tag {tag}")
    return int(rid)


def get_release_assets_numeric_ids(tag: str) -> Dict[str, int]:
    obj = _release_view_json(tag)
    assets = obj.get("assets") or []
    out: Dict[str, int] = {}
    if isinstance(assets, list):
        for a in assets:
            if not isinstance(a, dict):
                continue
            name = str(a.get("name", "")).strip()
            aid = a.get("id")
            if name and aid is not None:
                out[name] = int(aid)
    return out
