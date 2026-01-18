from __future__ import annotations

"""CSV-backed run-state store.

Implementation is loaded from chunk files to keep logic files <= 500 lines.
"""

from typing import Any, Dict

from ._runstate_csv.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()

for _k, _v in list(_NS.items()):
    if _k.startswith("__"):
        continue
    globals()[_k] = _v

__all__ = [k for k in _NS.keys() if not k.startswith("__")]
