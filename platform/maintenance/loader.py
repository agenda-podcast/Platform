from __future__ import annotations

from typing import Any, Dict

from ._builder_chunks.chunk_01 import get_chunk as _chunk_01
from ._builder_chunks.chunk_02 import get_chunk as _chunk_02
from ._builder_chunks.chunk_03 import get_chunk as _chunk_03
from ._builder_chunks.chunk_04 import get_chunk as _chunk_04

def load_namespace(package: str) -> Dict[str, Any]:
    code = "".join([_chunk_01(), _chunk_02(), _chunk_03(), _chunk_04()])
    ns: Dict[str, Any] = {"__name__": package + '._impl', "__package__": package}
    exec(code, ns, ns)
    return ns

