#!/usr/bin/env python3
from __future__ import annotations

try:
    from repo_bootstrap import ensure_repo_root_on_sys_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts.repo_bootstrap import ensure_repo_root_on_sys_path

ensure_repo_root_on_sys_path()

import sys
from pathlib import Path

# Ensure repo root is on sys.path so local 'platform' package wins over stdlib 'platform' module.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

from platform.config.load_platform_config import load_platform_config


def main() -> int:
    cfg = load_platform_config(_REPO_ROOT)
    print("[SMOKE][OK] platform_config loaded")
    print(f"[SMOKE][OK] top_level_keys={sorted(list(cfg.keys()))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
