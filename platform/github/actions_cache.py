from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import List


def _run(cmd: List[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


@dataclass
class ActionsCacheEntry:
    id: int
    key: str
    ref: str
    size_in_bytes: int
    created_at: str
    last_accessed_at: str


def list_caches() -> List[ActionsCacheEntry]:
    """List GitHub Actions caches (repository-scoped)."""
    cp = _run(["gh", "api", "-H", "Accept: application/vnd.github+json", "/repos/{owner}/{repo}/actions/caches"])
    if cp.returncode != 0:
        # Fallback: rely on gh auto-owner detection if templating is not expanded by gh.
        cp = _run(["gh", "api", "-H", "Accept: application/vnd.github+json", "repos/:owner/:repo/actions/caches"])
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to list caches: {cp.stderr.strip()}")
    data = json.loads(cp.stdout)
    caches = data.get("actions_caches", []) or data.get("actions_caches", [])
    out: List[ActionsCacheEntry] = []
    for c in caches:
        out.append(ActionsCacheEntry(
            id=int(c.get("id")),
            key=str(c.get("key")),
            ref=str(c.get("ref")),
            size_in_bytes=int(c.get("size_in_bytes", 0)),
            created_at=str(c.get("created_at")),
            last_accessed_at=str(c.get("last_accessed_at")),
        ))
    return out


def delete_cache(cache_id: int) -> None:
    cp = _run(["gh", "api", "-X", "DELETE", "repos/:owner/:repo/actions/caches/{id}".format(id=cache_id)])
    if cp.returncode != 0:
        raise RuntimeError(f"Failed to delete cache {cache_id}: {cp.stderr.strip()}")
