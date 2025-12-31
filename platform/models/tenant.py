from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Tenant:
    tenant_id: str
    tenant_name: str
    registered_at: str
    billing: Dict[str, Any] = field(default_factory=dict)
    allow_release_consumers: List[str] = field(default_factory=list)
