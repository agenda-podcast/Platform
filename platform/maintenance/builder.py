from __future__ import annotations

"""Maintenance builder entrypoint.

Implementation is loaded from role-based parts to keep each logic file at or
under 500 lines without mechanical chunk naming.
"""

from typing import Any, Dict

from ._builder.loader import load_namespace as _load_namespace

_NS: Dict[str, Any] = _load_namespace()

run_maintenance = _NS["run_maintenance"]

__all__ = ["run_maintenance"]
