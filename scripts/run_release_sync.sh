#!/usr/bin/env bash
set -euo pipefail

# This wrapper lets the workflow stay stable even if the underlying implementation
# lives in different places (module CLI vs scripts/*). It tries known entrypoints.

if [[ -f "scripts/release_sync.py" ]]; then
  exec python scripts/release_sync.py "$@"
elif [[ -f "scripts/sync_releases.py" ]]; then
  exec python scripts/sync_releases.py "$@"
else
  # Fallback: CLI entry (if present)
  if python -m platform.cli --help 2>/dev/null | grep -qi "sync"; then
    exec python -m platform.cli sync-releases "$@"
  fi
fi

echo "[ERROR] No release-sync entrypoint found. Expected scripts/release_sync.py or scripts/sync_releases.py, or a platform.cli 'sync-releases' command." >&2
exit 2
