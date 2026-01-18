from __future__ import annotations

"""Consistency and integrity validation.

Implementation is loaded from chunk files to keep logic files <= 500 lines.
"""

from typing import Any, Dict

from ._validator.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()

# Re-export all public symbols (non-dunder) from the loaded implementation.
for _k, _v in list(_NS.items()):
    if _k.startswith("__"):
        continue
    globals()[_k] = _v

__all__ = [k for k in _NS.keys() if not k.startswith("__")]
