from __future__ import annotations

"""Maintenance builder entrypoint.

Implementation is loaded from chunk files to keep logic files <= 500 lines.
"""

from typing import Any, Dict

from ._builder.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()

run_maintenance = _NS["run_maintenance"]

__all__ = ["run_maintenance"]
