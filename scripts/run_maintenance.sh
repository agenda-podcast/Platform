#!/usr/bin/env bash
set -euo pipefail

# Default locations (can be overridden by workflow env)
export BILLING_TAG="${BILLING_TAG:-billing-state-v1}"
export BILLING_TEMPLATE_DIR="${BILLING_TEMPLATE_DIR:-releases/billing-state-v1}"
export BILLING_STATE_DIR="${BILLING_STATE_DIR:-.billing-state}"

echo "[maintenance] Ensure billing-state release + bootstrap local billing-state"
python -m platform.maintenance.main

echo "[maintenance] Regenerate maintenance-state/ derived indexes"
python -m platform.cli maintenance
