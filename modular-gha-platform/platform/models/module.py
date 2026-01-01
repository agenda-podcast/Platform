from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ModuleCacheConfig:
    enabled: bool = False
    retention_default: str = "1w"
    key_inputs: List[str] = field(default_factory=list)


@dataclass
class ModuleDef:
    module_id: str
    version: str
    description: str = ""
    supports_downloadable_artifacts: bool = False
    produces_manifest: bool = False
    depends_on: List[str] = field(default_factory=list)
    cache: ModuleCacheConfig = field(default_factory=ModuleCacheConfig)
    inputs: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[Dict[str, Any]] = field(default_factory=list)
