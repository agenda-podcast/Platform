# Patch: Billing-state hydration (Release-first, best-effort)

## What this patch changes
- Adds `scripts/billing_state_hydrate.py`: hydrates the local `--billing-state-dir` **before** orchestrator validates it.
  - Best-effort download from GitHub Release tag `billing-state-v1` **per-file**
  - Fallback per-file to scaffold dir (defaults to `.billing-state-ci`)
  - Fails only if required files are missing in BOTH places
  - Never writes anything back into the repo scaffold; never fabricates empty CSVs

- Updates `scripts/run_orchestrator.sh` to call the hydration step automatically.

- Adds `scripts/e2e_assert_billing_state_hydration.py` to cover the new behavior.

## How to apply
1) Unzip over repository root (overwrite the listed files).
2) Ensure your workflows call `bash scripts/run_orchestrator.sh` (recommended).
3) Add E2E step:
   `python scripts/e2e_assert_billing_state_hydration.py`

## Optional env vars
- `BILLING_STATE_DIR` (default: ./.billing-state)
- `BILLING_SCAFFOLD_DIR` (default: ./.billing-state-ci if present)
- `BILLING_RELEASE_TAG` (default: billing-state-v1)
- `BILLING_GH_REPO` (default: $GITHUB_REPOSITORY)
