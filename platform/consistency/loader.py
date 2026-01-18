from __future__ import annotations

from typing import Any, Dict

from ._validator_parts.rules_table import get_part as _rules_table


def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([
        _rules_table(),
    ])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns
