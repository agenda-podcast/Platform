from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _repo_args() -> List[str]:
    repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    return ["--repo", repo] if repo and "/" in repo else []


def _run(cmd: List[str], *, cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
        cwd=str(cwd) if cwd is not None else None,
    )


def _require_ok(cp: subprocess.CompletedProcess, *, context: str) -> None:
    if cp.returncode != 0:
        stderr = (cp.stderr or "").strip()
        stdout = (cp.stdout or "").strip()
        detail = stderr if stderr else stdout
        raise RuntimeError(f"{context} (exit={cp.returncode}): {detail}")


def release_exists(tag: str, *, repo_root: Optional[Path] = None) -> bool:
    cp = _run(["gh", "release", "view", tag, *_repo_args()], cwd=repo_root)
    return cp.returncode == 0


def ensure_release(tag: str, *, title: Optional[str] = None, notes: Optional[str] = None, repo_root: Optional[Path] = None) -> None:
    if release_exists(tag, repo_root=repo_root):
        return
    cmd = ["gh", "release", "create", tag, *_repo_args()]
    if title:
        cmd += ["--title", title]
    if notes:
        cmd += ["--notes", notes]
    else:
        cmd += ["--notes", f"Automated release for {tag}"]
    cp = _run(cmd, cwd=repo_root)
    _require_ok(cp, context=f"Failed to create release {tag}")


def _release_view_json(tag: str, *, repo_root: Optional[Path] = None) -> Dict[str, object]:
    cp = _run(
        ["gh", "release", "view", tag, "--json", "id,tagName,assets", *_repo_args()],
        cwd=repo_root,
    )
    _require_ok(cp, context=f"Failed to view release {tag}")
    try:
        obj = json.loads(cp.stdout or "{}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse release view JSON for {tag}: {e}") from e
    if not isinstance(obj, dict):
        raise RuntimeError(f"Release view JSON for {tag} is not an object")
    return obj


def list_release_assets(tag: str, *, repo_root: Optional[Path] = None) -> List[Dict[str, object]]:
    obj = _release_view_json(tag, repo_root=repo_root)
    assets = obj.get("assets") or []
    if not isinstance(assets, list):
        return []
    out: List[Dict[str, object]] = []
    for a in assets:
        if isinstance(a, dict):
            out.append(a)
    return out


def get_release_assets_numeric_ids(tag: str, *, repo_root: Optional[Path] = None) -> Dict[str, int]:
    assets = list_release_assets(tag, repo_root=repo_root)
    out: Dict[str, int] = {}
    for a in assets:
        name = str(a.get("name", "")).strip()
        aid = a.get("id")
        if not name or aid is None:
            continue
        try:
            out[name] = int(aid)
        except (TypeError, ValueError):
            continue
    return out


def upload_release_assets(
    tag: str,
    files: Iterable[Path],
    clobber: bool = True,
    *,
    repo_root: Optional[Path] = None,
) -> None:
    ensure_release(tag, repo_root=repo_root)
    paths = [Path(p) for p in files]
    if not paths:
        return
    cmd = ["gh", "release", "upload", tag, *_repo_args(), *[str(p) for p in paths]]
    if clobber:
        cmd.append("--clobber")
    cp = _run(cmd, cwd=repo_root)
    _require_ok(cp, context=f"Failed to upload release assets for {tag}")


def download_release_assets(
    tag: str,
    *,
    dest_dir: Path,
    patterns: List[str],
    repo_root: Optional[Path] = None,
    clobber: bool = True,
) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    ensure_release(tag, repo_root=repo_root)
    for pat in patterns:
        cmd = ["gh", "release", "download", tag, *_repo_args(), "-D", str(dest_dir), "-p", pat]
        if clobber:
            cmd.append("--clobber")
        cp = _run(cmd, cwd=repo_root)
        _require_ok(cp, context=f"Failed to download release assets for {tag} pattern={pat}")


def delete_release_asset(
    tag: str,
    *,
    asset_name: str,
    repo_root: Optional[Path] = None,
) -> None:
    repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if not repo or "/" not in repo:
        raise RuntimeError("GITHUB_REPOSITORY is required to delete release assets via gh api")
    ids = get_release_assets_numeric_ids(tag, repo_root=repo_root)
    if asset_name not in ids:
        return
    asset_id = ids[asset_name]
    cmd = ["gh", "api", "-X", "DELETE", f"repos/{repo}/releases/assets/{asset_id}"]
    cp = _run(cmd, cwd=repo_root)
    _require_ok(cp, context=f"Failed to delete release asset {asset_name} for {tag}")
