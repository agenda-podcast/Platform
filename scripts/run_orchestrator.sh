#!/usr/bin/env bash
set -euo pipefail

# This wrapper is used by GitHub Actions.
# We enable GitHub Releases integration automatically in CI (where GH_TOKEN is set)
# while keeping local/offline runs unchanged unless explicitly requested.
EXTRA_ARGS=()
if [[ "${GITHUB_ACTIONS:-}" == "true" && -n "${GH_TOKEN:-}" ]]; then
  if [[ " ${*:-} " != *" --enable-github-releases "* ]]; then
    EXTRA_ARGS+=("--enable-github-releases")
  fi
fi

python -m platform.cli orchestrator \
  --billing-state-dir .billing-state \
  --runtime-dir runtime \
  --tenants-dir tenants \
  "${EXTRA_ARGS[@]}" \
  "$@"
