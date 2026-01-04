"""DEPRECATED.

Release sync is now integrated into the orchestrator.

This file intentionally exists only to prevent legacy workflows from failing
hard with Python import errors. It is expected that CI/workflows stop calling
this script directly.
"""

from __future__ import annotations

import sys


def main() -> int:
    sys.stderr.write(
        "[DEPRECATED] scripts/release_sync.py is no longer supported. "
        "Release synchronization runs automatically inside the orchestrator "
        "when the tenant/workorder purchased 'artifacts_download'.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
