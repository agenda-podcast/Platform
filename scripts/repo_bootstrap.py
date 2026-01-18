"""Runtime bootstrap for scripts executed as files.

Problem:
- GitHub Actions frequently runs scripts using `python scripts/<name>.py`.
- In that execution mode, Python sets sys.path[0] to the scripts directory,
  not the repository root.
- This breaks imports of the Platform package (platform/*) and may fall back
  to the stdlib `platform` module.

This module is intentionally tiny and role-scoped:
- It only ensures the repository root is present on sys.path.
- It does not implement business logic.
"""

from __future__ import annotations

from pathlib import Path
import sys


def ensure_repo_root_on_sys_path() -> Path:
    """Ensure repository root is importable.

    Returns:
        repo_root Path that was added (or already present).
    """
    scripts_dir = Path(__file__).resolve().parent
    repo_root = scripts_dir.parent

    s_repo = str(repo_root)
    if s_repo not in sys.path:
        sys.path.insert(0, s_repo)

    return repo_root
