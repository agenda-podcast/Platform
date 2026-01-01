from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

MODULE_ID_RE = re.compile(r"^(\d{3})_")

REQUIRED_FILES = [
    "module.yaml",
    "tenant_params.schema.json",
    "schema/results.schema.json",
    "schema/report.schema.json",
]

def fail(msg: str) -> int:
    print(f"[CI_VERIFY][FAIL] {msg}")
    return 2

def ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    if not modules_dir.exists():
        return fail(f"Modules dir not found: {modules_dir}")

    # Check each module folder has numeric ID prefix
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        if not MODULE_ID_RE.match(mod_dir.name):
            return fail(f"Module folder missing numeric prefix: {mod_dir}")

        # Required files
        for rel in REQUIRED_FILES:
            if not (mod_dir / rel).exists():
                return fail(f"Missing required file in {mod_dir.name}: {rel}")

        # Ensure placeholders are not present after Maintenance
        # (This check can be run in post-maintenance phase.)
        for path in mod_dir.rglob("*"):
            if path.is_file() and path.suffix.lower() in {".py", ".yaml", ".yml", ".json", ".md", ".txt"}:
                try:
                    text = path.read_text(encoding="utf-8")
                except Exception:
                    continue
                if "__MODULE_ID__" in text or "__MODULE_PREFIX__" in text:
                    return fail(f"Placeholders not rewritten in {path}")

    ok("Module folders: numeric IDs + required files + placeholders rewritten")
    return 0

if __name__ == "__main__":
    sys.exit(main())
