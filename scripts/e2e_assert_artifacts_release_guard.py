#!/usr/bin/env python3
"""E2E assertion for the release artifacts CI guard.

Creates a temporary repo-like structure with a workorder that requests release
artifacts and verifies that running the guard without a GitHub token exits with
code 2 and emits a clear reason_key.

Safe for CI and offline runs.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    here = Path(__file__).resolve().parent
    guard = here / "artifacts_release_guard.py"
    if not guard.exists():
        print("[E2E][FAIL] artifacts_release_guard.py missing", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "tenants" / "aaaaaa" / "workorders").mkdir(parents=True, exist_ok=True)

        (root / "tenants" / "aaaaaa" / "workorders" / "bbbbbbbb.yml").write_text(
            """work_order_id: bbbbbbbb
tenant_id: aaaaaa
modules:
  - module_id: 001
    purchase_release_artifacts: true
""",
            encoding="utf-8",
        )

        env = os.environ.copy()
        env["PLATFORM_REPO_ROOT"] = str(root)
        env.pop("GH_TOKEN", None)
        env.pop("GITHUB_TOKEN", None)

        p = subprocess.run(
            [sys.executable, str(guard), "--enforce"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if p.returncode != 2:
            print("[E2E][FAIL] Expected exit code 2 when token missing", file=sys.stderr)
            print("stdout:", p.stdout, file=sys.stderr)
            print("stderr:", p.stderr, file=sys.stderr)
            return 2

        if "reason_key=artifacts_release_token_missing" not in p.stderr:
            print("[E2E][FAIL] Expected reason_key in stderr", file=sys.stderr)
            print("stderr:", p.stderr, file=sys.stderr)
            return 2

    print("[E2E][OK] artifacts release guard blocks missing token")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
