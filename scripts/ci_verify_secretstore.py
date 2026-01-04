#!/usr/bin/env python3
"""CI checks for encrypted secretstore pattern.

Intended usage:
- Maintenance workflow: validate guardrails + ensure template is current (DO NOT require encrypted file)
- Orchestrator workflow (protected branches/environments): require encrypted file and decrypt at runtime

Checks:
- plaintext `platform/secretstore/secretstore.json` is NOT present in repo
- template exists and is up-to-date (regenerate and compare)
- optionally enforce that `platform/secretstore/secretstore.json.gpg` exists

Usage:
  python scripts/ci_verify_secretstore.py                # maintenance-friendly
  python scripts/ci_verify_secretstore.py --require-gpg  # runtime/prod friendly
"""

from __future__ import annotations

import argparse
import filecmp
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLAIN = ROOT / "platform" / "secretstore" / "secretstore.json"
ENCRYPTED = ROOT / "platform" / "secretstore" / "secretstore.json.gpg"
TEMPLATE = ROOT / "platform" / "secretstore" / "secretstore.template.json"
GEN = ROOT / "scripts" / "secretstore_generate_template.py"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--require-gpg",
        action="store_true",
        help="Fail if encrypted secretstore.json.gpg is missing (use in orchestrator/prod workflows).",
    )
    args = ap.parse_args()

    if PLAIN.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] plaintext secretstore must not be committed: {PLAIN}")

    if not TEMPLATE.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] template missing: {TEMPLATE}")

    if args.require_gpg and not ENCRYPTED.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] encrypted secretstore missing: {ENCRYPTED}")

    # Verify template is current: regen and compare
    tmp = ROOT / "platform" / "secretstore" / ".secretstore.template.tmp.json"
    try:
        shutil.copy2(TEMPLATE, tmp)
        subprocess.check_call(["python", str(GEN)])
        same = filecmp.cmp(TEMPLATE, tmp, shallow=False)
        if not same:
            raise SystemExit(
                "[CI_VERIFY][FAIL] secretstore.template.json is out of date. "
                "Run scripts/secretstore_generate_template.py and commit the result."
            )
        print("[CI_VERIFY][OK] secretstore template up-to-date")
    finally:
        if tmp.exists():
            shutil.copy2(tmp, TEMPLATE)
            tmp.unlink(missing_ok=True)

    # Non-fatal guidance in maintenance mode
    if (not args.require_gpg) and (not ENCRYPTED.exists()):
        print("[CI_VERIFY][WARN] encrypted secretstore not found (expected until admin creates it): platform/secretstore/secretstore.json.gpg")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
