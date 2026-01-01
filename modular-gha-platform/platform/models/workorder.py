from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class WorkOrderModuleSpec:
    module_id: str
    params: Dict[str, Any] = field(default_factory=dict)
    reuse_output_type: str = "new"  # new|cache|release|assets
    purchase_release_artifacts: bool = False
    release_tag: Optional[str] = None
    assets_folder_name: Optional[str] = None
    cache_retention_override: Optional[str] = None


@dataclass
class WorkOrder:
    tenant_id: str
    work_order_id: str
    enabled: bool
    mode: str
    modules: List[WorkOrderModuleSpec]
    promotions: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
