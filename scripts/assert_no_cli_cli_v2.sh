#!/usr/bin/env bash
set -euo pipefail

# Helper you can run locally to verify you do NOT have the invalid action reference.
grep -R --line-number -E 'uses:\s*cli/cli@v2' .github/workflows && {
  echo "FOUND: uses: cli/cli@v2  -> remove it (invalid action ref)"; exit 1;
} || {
  echo "OK: no cli/cli@v2 references found."
}
