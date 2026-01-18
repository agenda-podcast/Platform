from __future__ import annotations

"""Consistency and integrity validation.

Implementation is assembled from role-oriented parts so that Python logic files
stay at or under 500 lines, while keeping a stable import surface.
"""

from typing import Any, Dict

from ._validator.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()

for _k, _v in list(_NS.items()):
    if _k.startswith("__"):
        continue
    globals()[_k] = _v

__all__ = [k for k in _NS.keys() if not k.startswith("__")]
