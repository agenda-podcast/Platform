#!/usr/bin/env bash
set -euo pipefail

# Centralized E2E verification entrypoint.
# Used by:
# - .github/workflows/e2e_verification.yml (manual)
# - .github/workflows/maintenance.yml (automatic)

BILLING_STATE_DIR="${BILLING_STATE_DIR:-.billing-state-ci}"
RUNTIME_DIR="${RUNTIME_DIR:-runtime}"

echo "[E2E] Running ci_verify.py --phase pre"
python scripts/ci_verify.py --phase pre --billing-state-dir "${BILLING_STATE_DIR}" --runtime-dir "${RUNTIME_DIR}"

echo "[E2E] Running orchestrator (if present)"
if [[ -f "scripts/run_orchestrator.sh" ]]; then
  bash scripts/run_orchestrator.sh
else
  echo "[E2E] scripts/run_orchestrator.sh not found; skipping orchestrator run."
fi

echo "[E2E] Running ci_verify.py --phase post"
python scripts/ci_verify.py --phase post --billing-state-dir "${BILLING_STATE_DIR}" --runtime-dir "${RUNTIME_DIR}"

echo "[E2E] OK"
