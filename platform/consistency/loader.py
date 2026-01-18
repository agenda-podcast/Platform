from __future__ import annotations

from typing import Any, Dict

from ._validator_parts.rules_table import get_part as _rules_table
from ._validator_parts.workorder_validation import get_part as _workorder_validation
from ._validator_parts.integrity_checks import get_part as _integrity_checks

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_rules_table(), _workorder_validation(), _integrity_checks()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

