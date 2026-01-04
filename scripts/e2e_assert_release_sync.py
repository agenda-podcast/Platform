"""E2E: Release sync entrypoints are import-safe.

Must not require GitHub auth; only checks that the entrypoints do not crash.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    out = (p.stdout or "") + (p.stderr or "")
    return p.returncode, out


def main() -> int:
    code, out = _run([sys.executable, "-m", "scripts.release_sync", "--help"])
    if code != 0:
        print(out)
        return 1

    code, out = _run([sys.executable, "scripts/release_sync.py", "--help"])
    if code != 0:
        print(out)
        return 1

    print("[E2E_ASSERT][OK] Release sync entrypoints are import-safe.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
