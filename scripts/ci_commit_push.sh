#!/usr/bin/env bash
set -euo pipefail

MSG=${1:-"chore: automated update"}
BRANCH=${2:-"main"}

# Ensure we are on the expected branch
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [[ "$current_branch" != "$BRANCH" ]]; then
  echo "[CI] Switching branch: $current_branch -> $BRANCH"
  git checkout "$BRANCH"
fi

# Configure commit identity
if ! git config user.email >/dev/null; then
  git config user.email "github-actions[bot]@users.noreply.github.com"
fi
if ! git config user.name >/dev/null; then
  git config user.name "github-actions[bot]"
fi

# If no changes, nothing to do
if git diff --quiet && git diff --cached --quiet; then
  echo "[CI] No changes to commit."
  exit 0
fi

git add -A
if git diff --cached --quiet; then
  echo "[CI] Nothing staged after add -A."
  exit 0
fi

git commit -m "$MSG"

# Rebase onto latest remote to avoid non-fast-forward failures

git fetch origin "$BRANCH"
# If rebase fails, abort and exit with clear message
if ! git rebase "origin/$BRANCH"; then
  echo "[CI][FAIL] Rebase onto origin/$BRANCH failed. Aborting." >&2
  git rebase --abort || true
  exit 2
fi

git push origin "HEAD:$BRANCH"
