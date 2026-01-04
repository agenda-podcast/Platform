"""Release sync utility.

This script mirrors GitHub Releases assets into the repository's `releases/` folder
(and maintains internal anti-enumeration ID mappings).

Safe invocations:
  - python -m scripts.release_sync ...
  - python scripts/release_sync.py ...   (legacy)

When invoked as a legacy standalone script, we bootstrap sys.path so imports succeed.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ is None or __package__ == "":
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

from scripts.release_sync_impl import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
