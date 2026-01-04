from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Any, List

class SecretstoreError(RuntimeError):
    pass

@dataclass(frozen=True)
class ModuleConfig:
    secrets: Dict[str, str]
    vars: Dict[str, str]

@dataclass(frozen=True)
class Secretstore:
    version: int
    modules: Dict[str, ModuleConfig]

    @staticmethod
    def load(path: str | Path) -> "Secretstore":
        p = Path(path)
        if not p.exists():
            raise SecretstoreError(f"Secretstore file not found: {p}")

        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise SecretstoreError("Secretstore JSON must be an object")

        version = int(data.get("version", 0))
        if version != 1:
            raise SecretstoreError(f"Unsupported secretstore version: {version}")

        modules_raw = data.get("modules")
        if not isinstance(modules_raw, dict):
            raise SecretstoreError("Secretstore JSON missing 'modules' object")

        modules: Dict[str, ModuleConfig] = {}
        for module_id, cfg in modules_raw.items():
            if not isinstance(module_id, str) or not isinstance(cfg, dict):
                continue
            secrets = cfg.get("secrets", {})
            vars_ = cfg.get("vars", {})
            if not isinstance(secrets, dict) or not isinstance(vars_, dict):
                raise SecretstoreError(f"Invalid module config for module_id={module_id}")
            modules[module_id] = ModuleConfig(
                secrets={str(k): str(v) for k, v in secrets.items()},
                vars={str(k): str(v) for k, v in vars_.items()},
            )

        return Secretstore(version=version, modules=modules)

    def get_module(self, module_id: str) -> Optional[ModuleConfig]:
        return self.modules.get(module_id)

def required_env_from_module_spec(spec: Dict[str, Any]) -> Dict[str, List[str]]:
    env = spec.get("env") or {}
    if not isinstance(env, dict):
        return {"secrets": [], "vars": []}
    secrets = env.get("secrets") or []
    vars_ = env.get("vars") or []
    if not isinstance(secrets, list):
        raise SecretstoreError("module.yml env.secrets must be a list")
    if not isinstance(vars_, list):
        raise SecretstoreError("module.yml env.vars must be a list")
    return {"secrets": [str(x) for x in secrets], "vars": [str(x) for x in vars_]}

def inject_module_env(module_id: str, module_spec: Dict[str, Any], secretstore: Secretstore, env: dict) -> None:
    """Inject secrets+vars into env for this module_id based on module.yml env declarations."""
    req = required_env_from_module_spec(module_spec)
    secrets_req = req["secrets"]
    vars_req = req["vars"]

    if not secrets_req and not vars_req:
        return

    mod = secretstore.get_module(module_id)
    if mod is None:
        raise SecretstoreError(f"Secretstore missing module entry: {module_id}")

    def _get_value(key: str) -> Optional[str]:
        if key in mod.secrets:
            return mod.secrets[key]
        if key in mod.vars:
            return mod.vars[key]
        return None

    for key in secrets_req + vars_req:
        val = _get_value(key)
        if val is None:
            raise SecretstoreError(f"Missing key in secretstore for module_id={module_id}: {key}")
        if val.strip() == "" or val.strip().upper() == "REPLACE_ME":
            raise SecretstoreError(f"Key not populated (placeholder) for module_id={module_id}: {key}")
        env[key] = val
