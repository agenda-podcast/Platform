#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

export BILLING_TAG="${BILLING_TAG:-billing-state-v1}"
export BILLING_TEMPLATE_DIR="${BILLING_TEMPLATE_DIR:-releases/billing-state-v1}"

python -m platform.billing.publish_default_billing_release
