\
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest(state_dir: Path, required_files: List[str], schema_version: str = "1") -> Dict:
    files = {}
    for fn in required_files:
        p = state_dir / fn
        if p.exists():
            files[fn] = {
                "sha256": _sha256_file(p),
                "bytes": p.stat().st_size,
            }
        else:
            files[fn] = None

    return {
        "schema_version": schema_version,
        "generated_at": _utc_iso(),
        "state_dir": str(state_dir),
        "required_files": required_files,
        "files": files,
    }


def write_manifest(state_dir: Path, required_files: List[str], manifest_name: str = "state_manifest.json") -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(state_dir, required_files)
    out = state_dir / manifest_name
    out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out
