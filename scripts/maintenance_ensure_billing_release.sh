#!/usr/bin/env bash
set -euo pipefail

# Ensures GitHub Release billing-state-v1 exists and has required CSV assets.
# Requires: python, requests dependency (already used by Platform scripts), and GITHUB_TOKEN env.

python -m platform.billing.publish_default_release
