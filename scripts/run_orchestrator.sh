#!/usr/bin/env bash
set -euo pipefail

# Guard/auto-enable Releases publishing for paid "artifacts download".
# - If any workorder requests release artifacts, require GH_TOKEN/GITHUB_TOKEN.
# - If token exists, auto-add --enable-github-releases (unless already provided).

ARGS=("$@")

if [[ -f "scripts/artifacts_release_guard.py" ]]; then
  NEED_RELEASES=$(python scripts/artifacts_release_guard.py --needs-releases-flag || echo "0")
  if [[ "${NEED_RELEASES}" == "1" ]]; then
    # Enforce token/gh availability.
    python scripts/artifacts_release_guard.py --enforce

    # Auto-enable releases if the orchestrator CLI supports it.
    if ! printf '%s\n' "${ARGS[@]}" | grep -q -- '--enable-github-releases'; then
      if python -m platform.cli orchestrator --help 2>/dev/null | grep -q -- '--enable-github-releases'; then
        ARGS+=(--enable-github-releases)
      fi
    fi
  fi
fi

if [[ -f "scripts/orchestrate.py" ]]; then
  exec python scripts/orchestrate.py "${ARGS[@]}"
elif [[ -f "scripts/orchestrator.py" ]]; then
  exec python scripts/orchestrator.py "${ARGS[@]}"
else
  if python -m platform.cli --help 2>/dev/null | grep -qi "orchestr"; then
    exec python -m platform.cli orchestrator "${ARGS[@]}"
  fi
fi

echo "[ERROR] No orchestrator entrypoint found." >&2
exit 2
