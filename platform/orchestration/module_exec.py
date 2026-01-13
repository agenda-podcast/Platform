from __future__ import annotations

import importlib.util
import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ..utils.hashing import sha256_file, short_hash
from ..utils.ids import validate_module_id


@dataclass
class ModuleExecResult:
    status: str  # COMPLETED|FAILED
    reason_code: Optional[str] = None
    cache_key: Optional[str] = None
    manifest_item: Optional[Dict[str, Any]] = None


def _import_module_runner(module_path: Path):
    runner_path = module_path / "src" / "run.py"
    if not runner_path.exists():
        raise FileNotFoundError(str(runner_path))
    spec = importlib.util.spec_from_file_location(f"module_{module_path.name}_runner", runner_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module runner: {runner_path}")
    # Allow module runners to import sibling helper modules from the same src/ directory.
    import sys

    src_dir = str(runner_path.parent)
    inserted = False
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
        inserted = True

    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    finally:
        if inserted:
            # Remove the first matching occurrence (we inserted at position 0).
            try:
                sys.path.remove(src_dir)
            except ValueError:
                pass
    if not hasattr(mod, "run"):
        raise AttributeError(f"Module runner must define run(params, outputs_dir): {runner_path}")
    return mod


def derive_cache_key(module_id: str, tenant_id: str, key_inputs: Dict[str, Any]) -> str:
    """Recommended cache key format v1|tenant=...|module=...|type=outputs|hash=..."""
    validate_module_id(module_id)
    # stable JSON for hashing
    payload = json.dumps(key_inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    import hashlib

    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:6]
    return f"v1|tenant={tenant_id}|module={module_id}|type=outputs|hash={h}"


def build_manifest_item(
    tenant_id: str,
    work_order_id: str,
    module_id: str,
    item_id: str,
    file_path: Path,
    mime_type: str,
) -> Dict[str, Any]:
    sha = sha256_file(file_path)
    sh = short_hash(sha)
    ext = file_path.suffix
    filename = f"{tenant_id}-{work_order_id}-{module_id}-{item_id}-{sh}{ext}"
    return {
        "filename": filename,
        "module_id": module_id,
        "item_id": item_id,
        "short_hash": sh,
        "sha256": sha,
        "size_bytes": str(file_path.stat().st_size),
        "mime_type": mime_type,
        "_source_path": str(file_path),
    }


def execute_module_runner(
    module_path: Path,
    params: Dict[str, Any],
    outputs_dir: Path,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    runner = _import_module_runner(module_path)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    # Inject env vars (secrets/vars) for the duration of this module run only.
    # This keeps secrets out of global process environment once the step finishes.
    old: Dict[str, Optional[str]] = {}
    if env:
        for k, v in env.items():
            if not k:
                continue
            old[k] = os.environ.get(k)
            os.environ[k] = str(v)
    try:
        return runner.run(params=params, outputs_dir=outputs_dir)
    finally:
        if env:
            for k in env.keys():
                if k not in old:
                    continue
                prev = old[k]
                if prev is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = prev
