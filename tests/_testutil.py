from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_on_path() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    # If the stdlib 'platform' module is loaded, remove it so our package can import
    if 'platform' in sys.modules and not hasattr(sys.modules['platform'], '__path__'):
        del sys.modules['platform']
    return repo_root
