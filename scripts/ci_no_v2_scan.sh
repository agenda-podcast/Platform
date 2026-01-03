#!/usr/bin/env bash
set -euo pipefail

# Fail-fast if any workflow references cli/cli@v2 or *any* @v2 action tag.
# This prevents the recurring failure: "Unable to resolve action `cli/cli@v2`, unable to find version `v2`".

echo "[CI] Scanning .github/workflows for forbidden action refs..."

FOUND=0

echo "[CI] Checking for 'uses: cli/cli@*'..."
if grep -RIn --exclude-dir=.git -E '^\s*uses:\s*cli/cli@' .github/workflows; then
  FOUND=1
fi

echo "[CI] Checking for any '@v2' action refs..."
if grep -RIn --exclude-dir=.git -E '^\s*uses:\s*[^ ]+@v2(\s|$)' .github/workflows; then
  FOUND=1
fi

if [[ "$FOUND" -ne 0 ]]; then
  echo "[CI][FAIL] Forbidden action ref(s) detected. Remove them and retry." >&2
  exit 2
fi

echo "[CI][OK] No forbidden action refs found."
