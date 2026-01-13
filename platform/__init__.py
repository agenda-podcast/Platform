from __future__ import annotations

"""PLATFORM project Python package.

This repository uses the package name ``platform``, which collides with Python's
standard library module ``platform``. Some third-party libraries (and parts of
the standard library, such as ``uuid``) import ``platform`` and expect the
stdlib API (for example, ``platform.system()``).

To keep this repository usable without renaming the package, we expose a small
compatibility surface by delegating the stdlib ``platform`` API from within
this package.
"""

from types import ModuleType
from typing import Optional


def _load_stdlib_platform() -> Optional[ModuleType]:
    try:
        import importlib.util
        import sysconfig
        from pathlib import Path

        stdlib_dir = Path(sysconfig.get_paths()["stdlib"])
        stdlib_platform_path = stdlib_dir / "platform.py"
        if not stdlib_platform_path.exists():
            return None

        spec = importlib.util.spec_from_file_location("_stdlib_platform", str(stdlib_platform_path))
        if spec is None or spec.loader is None:
            return None

        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    except Exception:
        return None


_stdlp = _load_stdlib_platform()


def system() -> str:
    if _stdlp is not None and hasattr(_stdlp, "system"):
        return str(_stdlp.system())
    import os
    return str(os.environ.get("OS", ""))


def node() -> str:
    if _stdlp is not None and hasattr(_stdlp, "node"):
        return str(_stdlp.node())
    import socket
    return str(socket.gethostname())


def release() -> str:
    if _stdlp is not None and hasattr(_stdlp, "release"):
        return str(_stdlp.release())
    return ""


def version() -> str:
    if _stdlp is not None and hasattr(_stdlp, "version"):
        return str(_stdlp.version())
    return ""


def machine() -> str:
    if _stdlp is not None and hasattr(_stdlp, "machine"):
        return str(_stdlp.machine())
    return ""


def python_version() -> str:
    if _stdlp is not None and hasattr(_stdlp, "python_version"):
        return str(_stdlp.python_version())
    import sys
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


__all__ = [
    "system",
    "node",
    "release",
    "version",
    "machine",
    "python_version",
]



def __getattr__(name: str):
    # Delegate unknown attributes to the stdlib platform module when available.
    if _stdlp is not None and hasattr(_stdlp, name):
        return getattr(_stdlp, name)
    raise AttributeError(f"module 'platform' has no attribute {name!r}")


def __dir__():
    base = set(globals().keys())
    if _stdlp is not None:
        try:
            base.update(dir(_stdlp))
        except Exception:
            pass
    return sorted(base)
