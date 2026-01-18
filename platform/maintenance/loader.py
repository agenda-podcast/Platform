from __future__ import annotations

from typing import Any, Dict

from ._builder_parts.modules_index import get_part as _part_01
from ._builder_parts.workorders_index import get_part as _part_02
from ._builder_parts.prices_and_requirements import get_part as _part_03
from ._builder_parts.billing_release_assets import get_part as _part_04

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_part_01(), _part_02(), _part_03(), _part_04()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

