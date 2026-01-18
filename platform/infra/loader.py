from __future__ import annotations

from typing import Any, Dict

from ._factory_chunks.chunk_01 import get_chunk as _chunk_01
from ._factory_chunks.chunk_02 import get_chunk as _chunk_02

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_chunk_01(), _chunk_02()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

