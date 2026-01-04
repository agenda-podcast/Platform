#!/usr/bin/env bash
set -euo pipefail

if [[ -f "scripts/admin_topup.py" ]]; then
  exec python scripts/admin_topup.py "$@"
elif [[ -f "scripts/topup_reconcile.py" ]]; then
  exec python scripts/topup_reconcile.py "$@"
else
  if python -m platform.cli --help 2>/dev/null | grep -qi "topup"; then
    exec python -m platform.cli admin-topup "$@"
  fi
fi

echo "[ERROR] No admin-topup entrypoint found." >&2
exit 2
