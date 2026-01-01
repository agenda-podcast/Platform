"""Modular GitHub Actions Platform.

This repository historically used a top-level Python package named ``platform``.
However, Python's standard library also has a module named ``platform`` and a
number of stdlib modules (notably :pymod:`uuid`) import it at runtime.

When the repository root is on ``sys.path`` (as it is in GitHub Actions), the
local package shadows the stdlib module, causing failures like:

``AttributeError: module 'platform' has no attribute 'system'``.

To preserve backwards compatibility (e.g., ``python -m platform.cli``) while
avoiding stdlib breakage, we load the *real* stdlib ``platform`` module from the
interpreter's stdlib path and re-export its public attributes into this package
namespace.

This makes ``import platform; platform.system()`` behave as expected for stdlib
callers, while keeping the project modules under ``platform.*``.
"""

from __future__ import annotations

import os
import sysconfig
import importlib.util
from types import ModuleType
from typing import Optional


def _load_stdlib_platform() -> Optional[ModuleType]:
    """Load the stdlib `platform.py` module under an internal alias.

    We cannot use a normal `import platform` here because that would recurse back
    into this package. Instead we locate `platform.py` in the stdlib directory
    and load it via an explicit module spec.
    """

    stdlib_dir = sysconfig.get_path("stdlib")
    if not stdlib_dir:
        return None

    candidate = os.path.join(stdlib_dir, "platform.py")
    if not os.path.isfile(candidate):
        return None

    spec = importlib.util.spec_from_file_location("_stdlib_platform", candidate)
    if spec is None or spec.loader is None:
        return None

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_stdlib_platform = _load_stdlib_platform()
if _stdlib_platform is not None:
    for _name in dir(_stdlib_platform):
        # Public API only (skip dunder/private names)
        if _name.startswith("_"):
            continue
        # Do not overwrite project-defined names
        if _name in globals():
            continue
        globals()[_name] = getattr(_stdlib_platform, _name)


__all__ = ["__version__"]
__version__ = "0.1.1"
