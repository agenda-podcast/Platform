#!/usr/bin/env bash
set -euo pipefail

# Wrapper to run orchestrator with correct billing-state hydration:
# 1) Best-effort download billing-state assets from Release billing-state-v1
# 2) Fallback to repo scaffold (.billing-state-ci) per file
# 3) Fail only if required files missing in both places
#
# This does NOT copy anything back into the repository scaffold.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BILLING_STATE_DIR="${BILLING_STATE_DIR:-${ROOT_DIR}/.billing-state}"
RUNTIME_DIR="${RUNTIME_DIR:-${ROOT_DIR}/runtime}"

mkdir -p "${RUNTIME_DIR}"
mkdir -p "${BILLING_STATE_DIR}"

# Hydrate local billing state BEFORE orchestrator validates it.
python "${ROOT_DIR}/scripts/billing_state_hydrate.py" \
  --billing-state-dir "${BILLING_STATE_DIR}" \
  ${BILLING_SCAFFOLD_DIR:+--scaffold-dir "${BILLING_SCAFFOLD_DIR}"} \
  ${BILLING_RELEASE_TAG:+--release-tag "${BILLING_RELEASE_TAG}"} \
  ${BILLING_GH_REPO:+--repo "${BILLING_GH_REPO}"}

# Run orchestrator
python -m platform.cli orchestrator \
  --runtime-dir "${RUNTIME_DIR}" \
  --billing-state-dir "${BILLING_STATE_DIR}" \
  "${@}"
