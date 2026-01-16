from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from platform.utils.yamlio import read_yaml

from .validate_platform_config import validate_platform_config


DEFAULT_CONFIG_REL_PATH = Path("platform/config/platform_config.yml")


def load_platform_config(repo_root: Path) -> Dict[str, Any]:
    """Load and validate platform/config/platform_config.yml.

    Contract:
    - Deterministic parsing.
    - Strict validation: missing keys or unknown keys fail fast.
    - No hidden defaults beyond the YAML file.

    Args:
        repo_root: repository root path.

    Returns:
        Parsed config dict.

    Raises:
        FileNotFoundError: if the config file is missing.
        ValueError: if the config file is invalid.
    """

    root = Path(repo_root)
    path = root / DEFAULT_CONFIG_REL_PATH
    if not path.exists():
        raise FileNotFoundError(f"Missing platform config: {path}")

    data = read_yaml(path)
    validate_platform_config(data)
    return data
