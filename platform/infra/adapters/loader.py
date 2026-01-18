from __future__ import annotations

from typing import Any, Dict

from ._runstate_csv_parts.runstate_reader import get_part as _part_runstate_reader
from ._runstate_csv_parts.runstate_writer import get_part as _part_runstate_writer

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_part_runstate_reader(), _part_runstate_writer()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

