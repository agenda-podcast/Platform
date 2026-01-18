from __future__ import annotations

from typing import Any, Dict

from ._runstate_csv_parts.runstate_read_write import get_part as _part_01
from ._runstate_csv_parts.evidence_and_pricing import get_part as _part_02

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_part_01(), _part_02()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

