from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

import yaml

from .loader import SecretStore, env_for_module


def load_module_yaml_from_repo(repo_root: Path, module_id: str) -> Dict[str, Any]:
    """Load modules/<module_id>/module.yml from the repository."""
    p = repo_root / "modules" / str(module_id) / "module.yml"
    if not p.exists():
        return {}
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def required_secret_names(module_yaml: Dict[str, Any]) -> List[str]:
    """Extract required secret env var names from module.yml requirements.secrets."""
    req = module_yaml.get("requirements") or {}
    if not isinstance(req, dict):
        return []

    secrets = req.get("secrets") or []
    if not isinstance(secrets, list):
        return []

    out: List[str] = []
    seen: Set[str] = set()

    for it in secrets:
        name: str = ""
        if isinstance(it, str):
            name = it.strip()
        elif isinstance(it, dict):
            name = str(it.get("name") or "").strip()
        if not name:
            continue
        if name in seen:
            continue
        out.append(name)
        seen.add(name)

    return out


def validate_required_secrets_for_modules(
    *,
    load_module_yaml_fn: Callable[[str], Dict[str, Any]],
    store: SecretStore,
    module_ids: Iterable[str],
    env: Optional[Dict[str, str]] = None,
    offline_ok: bool = False,
) -> Dict[str, List[str]]:
    """Return missing required secrets per module.

    A secret is considered satisfied when it is available as a non-empty value from either:
      - the current process environment (os.environ)
      - injected module env from secretstore (env_for_module)

    When offline_ok is True, this returns an empty dict.
    """
    if offline_ok:
        return {}

    env_map = env if env is not None else dict(os.environ)

    missing_by_module: Dict[str, List[str]] = {}

    for module_id in sorted({str(m).strip() for m in module_ids if str(m).strip()}):
        module_yaml = load_module_yaml_fn(module_id)
        required = required_secret_names(module_yaml)
        if not required:
            continue

        injected = env_for_module(store, module_id)

        missing: List[str] = []
        for name in required:
            v1 = str(env_map.get(name, "") or "").strip()
            v2 = str(injected.get(name, "") or "").strip()

            if v1 or v2:
                continue

            # Back-compat: allow module-prefixed env keys.
            pref = f"{module_id}_{name}"
            v3 = str(env_map.get(pref, "") or "").strip()
            v4 = str(injected.get(pref, "") or "").strip()
            if v3 or v4:
                continue

            missing.append(name)

        if missing:
            missing_by_module[module_id] = missing

    return missing_by_module
