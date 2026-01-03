#!/usr/bin/env bash
set -euo pipefail

# Default behavior: enable GitHub Releases unless caller explicitly set flag.
# This makes orchestrator runs actually publish/update release artifacts, which is critical
# for seeing work happening in Actions.
args=("$@")
has_enable=0
for a in "${args[@]}"; do
  if [[ "$a" == "--enable-github-releases" ]]; then
    has_enable=1
    break
  fi
done
if [[ $has_enable -eq 0 ]]; then
  args=(--enable-github-releases "${args[@]}")
fi

if [[ -f "scripts/orchestrate.py" ]]; then
  exec python scripts/orchestrate.py "${args[@]}"
elif [[ -f "scripts/orchestrator.py" ]]; then
  exec python scripts/orchestrator.py "${args[@]}"
else
  if python -m platform.cli --help 2>/dev/null | grep -qi "orchestr"; then
    exec python -m platform.cli orchestrator "${args[@]}"
  fi
fi

echo "[ERROR] No orchestrator entrypoint found." >&2
exit 2
