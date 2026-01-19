#!/usr/bin/env bash
set -euo pipefail

# Orchestrator wrapper used by CI and local runs.
#
# Responsibilities:
#  1) Hydrate billing-state from GitHub Release (billing-state-v1) with repo-scaffold fallback.
#  2) Auto-enable GitHub Releases in orchestrator when 'download artifacts' was purchased.
#  3) Publish updated billing-state back to the fixed Release tag (billing-state-v1).
#
# Notes:
#  - This script intentionally does NOT copy billing-state assets back into the repo.
#  - Offline/local runs will no-op the GitHub steps.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BILLING_STATE_DIR="${BILLING_STATE_DIR:-${ROOT_DIR}/.billing-state}"
BILLING_SCAFFOLD_DIR="${BILLING_SCAFFOLD_DIR:-${ROOT_DIR}/.billing-state-ci}"
RUNTIME_DIR="${RUNTIME_DIR:-${ROOT_DIR}/runtime}"
BILLING_RELEASE_TAG="${BILLING_RELEASE_TAG:-billing-state-v1}"

mkdir -p "${RUNTIME_DIR}" "${BILLING_STATE_DIR}"

# Ensure GH_TOKEN is set when running inside GitHub Actions.
# Our GitHub API utilities commonly accept GH_TOKEN.
if [[ -z "${GH_TOKEN:-}" && -n "${GITHUB_TOKEN:-}" ]]; then
  export GH_TOKEN="${GITHUB_TOKEN}"
fi

# 1) Hydrate billing-state assets locally (release-first, scaffold fallback).
python "${ROOT_DIR}/scripts/billing_state_hydrate.py" \
  --billing-state-dir "${BILLING_STATE_DIR}" \
  --scaffold-dir "${BILLING_SCAFFOLD_DIR}" \
  --release-tag "${BILLING_RELEASE_TAG}"

# 2) Detect whether artifacts download was purchased anywhere in repo workorders.
ENABLE_RELEASES_FLAG=""
# Capture 1/0 reliably.
PURCHASED="$(python - <<'PY'
import sys
from pathlib import Path

try:
    import yaml
except Exception:
    print("0")
    sys.exit(0)

root = Path(__file__).resolve().parent.parent
tenants_dir = root / "tenants"

def truthy(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1","true","yes","y","on"}

signals = {
    "purchase_release_artifacts",
    "purchase_artifacts_download",
    "artifacts_download",
    "artifacts_download_purchased",
    "artifact_download_purchased",
}

def has_purchase(w: dict) -> bool:
    for k in signals:
        if truthy(w.get(k)):
            return True
    purchases = w.get("purchases") or w.get("purchased_features")
    if isinstance(purchases, str):
        purchases = [purchases]
    if isinstance(purchases, list):
        norm = {str(x).strip().lower() for x in purchases}
        if any(x in norm for x in ("artifacts_download","artifacts-download","artifacts","release_artifacts")):
            return True
    for m in (w.get("modules") or []):
        if not isinstance(m, dict):
            continue
        for k in signals:
            if truthy(m.get(k)):
                return True
    return False

if not tenants_dir.exists():
    print("0")
    sys.exit(0)

for tdir in tenants_dir.iterdir():
    if not tdir.is_dir():
        continue
    wdir = tdir / "workorders"
    if not wdir.exists():
        continue
    for wp in wdir.glob("*.yml"):
        try:
            w = yaml.safe_load(wp.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if isinstance(w, dict) and has_purchase(w):
            print("1")
            sys.exit(0)

print("0")
PY
)"

if [[ "${PURCHASED}" == "1" ]]; then
  ENABLE_RELEASES_FLAG="--enable-github-releases"
fi

# In GitHub Actions, enabling Releases is required for delivery modules such as deliver_github_release.
# This avoids relying on heuristic purchase detection.
if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  ENABLE_RELEASES_FLAG="--enable-github-releases"
fi

# 3) Run orchestrator.
python -m platform.cli orchestrator \
  --runtime-dir "${RUNTIME_DIR}" \
  --billing-state-dir "${BILLING_STATE_DIR}" \
  ${ENABLE_RELEASES_FLAG} \
  "$@"

# 4) Publish billing-state back to Releases (always in CI, best-effort).
python "${ROOT_DIR}/scripts/billing_state_publish.py" \
  --billing-state-dir "${BILLING_STATE_DIR}" \
  --release-tag "${BILLING_RELEASE_TAG}" \
  ${BILLING_GH_REPO:+--repo "${BILLING_GH_REPO}"}
