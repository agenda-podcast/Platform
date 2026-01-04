from __future__ import annotations

import importlib.util
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
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
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


def execute_module_runner(module_path: Path, params: Dict[str, Any], outputs_dir: Path, env_overrides: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Execute a module runner with optional environment overrides.

    Modules are allowed to read configuration from environment variables. To avoid
    modifying module code, orchestrator can inject required keys for the duration
    of the module run, and then restore the previous environment.
    """
    import os

    runner = _import_module_runner(module_path)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    if not env_overrides:
        return runner.run(params=params, outputs_dir=outputs_dir)

    # Apply overrides (save old values and restore after)
    old: Dict[str, Optional[str]] = {}
    try:
        for k, v in env_overrides.items():
            old[k] = os.environ.get(k)
            os.environ[k] = str(v)
        return runner.run(params=params, outputs_dir=outputs_dir)
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev
