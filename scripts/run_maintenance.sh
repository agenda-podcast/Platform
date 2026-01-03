#!/usr/bin/env bash
set -euo pipefail

if [[ -f "scripts/maintenance.py" ]]; then
  exec python scripts/maintenance.py "$@"
else
  if python -m platform.cli --help 2>/dev/null | grep -qi "maint"; then
    exec python -m platform.cli maintenance "$@"
  fi
fi

echo "[ERROR] No maintenance entrypoint found." >&2
exit 2
