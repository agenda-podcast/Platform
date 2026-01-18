from __future__ import annotations

from typing import Any, Dict

from ._builder_parts.modules_registry import get_part as _part_modules_registry
from ._builder_parts.workorders_registry import get_part as _part_workorders_registry
from ._builder_parts.requirements_and_ports import get_part as _part_requirements_and_ports
from ._builder_parts.reasons_and_indexes import get_part as _part_reasons_and_indexes

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_part_modules_registry(), _part_workorders_registry(), _part_requirements_and_ports(), _part_reasons_and_indexes()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

