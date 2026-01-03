#!/usr/bin/env bash
set -euo pipefail

if [[ -f "scripts/orchestrate.py" ]]; then
  exec python scripts/orchestrate.py "$@"
elif [[ -f "scripts/orchestrator.py" ]]; then
  exec python scripts/orchestrator.py "$@"
else
  if python -m platform.cli --help 2>/dev/null | grep -qi "orchestr"; then
    exec python -m platform.cli orchestrator "$@"
  fi
fi

echo "[ERROR] No orchestrator entrypoint found." >&2
exit 2
