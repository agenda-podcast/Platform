#!/usr/bin/env python3
"""Generate platform/secretstore/secretstore.template.json from module manifests.

- Scans ./modules/*/module.yaml
- Collects config.secrets and config.vars per module_id (case-sensitive)
- Produces a deterministic JSON template with REPLACE_ME placeholders
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Any

try:
    import yaml  # type: ignore
except Exception as e:
    raise SystemExit("Missing dependency: pyyaml. Add it to your requirements or install in CI.") from e

ROOT = Path(__file__).resolve().parents[1]
MODULES_DIR = ROOT / "modules"
OUT_PATH = ROOT / "platform" / "secretstore" / "secretstore.template.json"
PLACEHOLDER = "REPLACE_ME"

def _load_module_manifest(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML (expected mapping): {path}")
    for k in ("module_id", "config"):
        if k not in data:
            raise ValueError(f"Missing '{k}' in {path}")
    cfg = data.get("config") or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"'config' must be a mapping in {path}")
    for section in ("secrets", "vars"):
        if section not in cfg:
            raise ValueError(f"Missing config.{section} in {path}")
        if not isinstance(cfg[section], list):
            raise ValueError(f"config.{section} must be a list in {path}")
    return data

def main() -> int:
    if not MODULES_DIR.exists():
        raise SystemExit(f"Modules dir not found: {MODULES_DIR}")

    modules: Dict[str, Dict[str, Dict[str, str]]] = {}
    for module_path in sorted(MODULES_DIR.iterdir()):
        if not module_path.is_dir():
            continue
        manifest = module_path / "module.yaml"
        if not manifest.exists():
            continue
        data = _load_module_manifest(manifest)
        module_id = str(data["module_id"])
        cfg = data["config"]

        secrets: List[str] = sorted(set(str(x) for x in (cfg.get("secrets") or [])))
        vars_: List[str] = sorted(set(str(x) for x in (cfg.get("vars") or [])))

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
