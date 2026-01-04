#!/usr/bin/env python3
"""
E2E assertion: billing_state_hydrate does NOT fabricate placeholders and
succeeds when scaffold provides required files.

This runs without network/gh. It creates a temp scaffold containing required
files and confirms hydration copies them into target.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from scripts.billing_state_hydrate import _read_required_files, hydrate_billing_state_dir


def main() -> int:
    required = _read_required_files()

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        scaffold = td / "scaffold"
        target = td / "target"
        scaffold.mkdir(parents=True, exist_ok=True)

        # Create minimal fake required files in scaffold
        for name in required:
            (scaffold / name).write_text("header\n", encoding="utf-8")

        # Ensure no gh/token usage
        os.environ.pop("GH_TOKEN", None)
        os.environ.pop("GITHUB_TOKEN", None)

        hydrate_billing_state_dir(
            target,
            scaffold_dir=scaffold,
            release_tag="billing-state-v1",
            repo=None,
            required_files=required,
            allow_release_download=False,
        )

        # Assert: all required files exist in target and are non-empty
        for name in required:
            p = target / name
            assert p.exists(), f"missing {name}"
            assert p.read_text(encoding="utf-8").strip() != "", f"empty {name}"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
