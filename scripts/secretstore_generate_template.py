#!/usr/bin/env python3
"""Generate platform/secretstore/secretstore.template.json from module specs.

Source of truth: `modules/*/module.yml`

- Reads `env.secrets` and `env.vars` from each module.yml (case-sensitive module_id and key names)
- Produces deterministic JSON with REPLACE_ME placeholders
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODULES_DIR = ROOT / "modules"
OUT_PATH = ROOT / "platform" / "secretstore" / "secretstore.template.json"
PLACEHOLDER = "REPLACE_ME"

def _load_module_spec(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML (expected mapping): {path}")
    if "module_id" not in data:
        raise ValueError(f"Missing module_id in {path}")
    return data

def _as_list(x: Any) -> List[str]:
    if x is None:
        return []
    if not isinstance(x, list):
        raise ValueError("Expected list")
    return [str(i) for i in x]

def main() -> int:
    if not MODULES_DIR.exists():
        raise SystemExit(f"Modules dir not found: {MODULES_DIR}")

    modules: Dict[str, Dict[str, Dict[str, str]]] = {}
    for module_path in sorted(MODULES_DIR.iterdir()):
        if not module_path.is_dir():
            continue
        spec_path = module_path / "module.yml"
        if not spec_path.exists():
            continue
        spec = _load_module_spec(spec_path)
        module_id = str(spec["module_id"])

        env = spec.get("env") or {}
        if not isinstance(env, dict):
            env = {}

        secrets = sorted(set(_as_list(env.get("secrets"))))
        vars_ = sorted(set(_as_list(env.get("vars"))))

        modules[module_id] = {
            "secrets": {k: PLACEHOLDER for k in secrets},
            "vars": {k: PLACEHOLDER for k in vars_},
        }

    out = {
        "version": 1,
        "generated_at": "CI" if os.environ.get("GITHUB_RUN_ID") else "LOCAL",
        "modules": {k: modules[k] for k in sorted(modules.keys())},
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote template: {OUT_PATH}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
