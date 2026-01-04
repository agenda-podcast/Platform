#!/usr/bin/env python3
"""CI checks for encrypted secretstore pattern.

Checks:
- plaintext secretstore.json is NOT present in repo
- encrypted secretstore.json.gpg exists (optional: enforce)
- template exists and is up-to-date (regenerate and compare)

Usage:
  python scripts/ci_verify_secretstore.py --enforce-encrypted
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
    ap.add_argument("--enforce-encrypted", action="store_true", help="Fail if encrypted file is missing.")
    args = ap.parse_args()

    if PLAIN.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] plaintext secretstore must not be committed: {PLAIN}")

    if not TEMPLATE.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] template missing: {TEMPLATE}")

    if args.enforce_encrypted and not ENCRYPTED.exists():
        raise SystemExit(f"[CI_VERIFY][FAIL] encrypted secretstore missing: {ENCRYPTED}")

    tmp = ROOT / "platform" / "secretstore" / ".secretstore.template.tmp.json"
    try:
        shutil.copy2(TEMPLATE, tmp)
        subprocess.check_call(["python", str(GEN)])
        same = filecmp.cmp(TEMPLATE, tmp, shallow=False)
        if not same:
            raise SystemExit("[CI_VERIFY][FAIL] secretstore.template.json is out of date. Run scripts/secretstore_generate_template.py and commit the result.")
        print("[CI_VERIFY][OK] secretstore template up-to-date")
    finally:
        if tmp.exists():
            shutil.copy2(tmp, TEMPLATE)
            tmp.unlink(missing_ok=True)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
