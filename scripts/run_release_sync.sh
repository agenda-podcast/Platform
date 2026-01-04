#!/usr/bin/env bash
set -euo pipefail
# Run as a package module so relative imports work.
python -m scripts.release_sync "$@"
