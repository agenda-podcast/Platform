# Patch: Billing-state hydration (Release-first, best-effort)

## What this patch changes
- Adds `scripts/billing_state_hydrate.py`: hydrates the local `--billing-state-dir` **before** orchestrator validates it.
  - Best-effort download from GitHub Release tag `billing-state-v1` **per-file**
  - Fallback per-file to scaffold dir (defaults to `.billing-state-ci`)
  - Fails only if required files are missing in BOTH places
  - Never writes anything back into the repo scaffold; never fabricates empty CSVs

- Updates `scripts/run_orchestrator.sh` to call the hydration step automatically.


Note: this repo no longer wires any E2E assertion scripts into CI. Verification workflows will be introduced later
after the core functionality stabilizes.

## How to apply
1) Unzip over repository root (overwrite the listed files).
2) Ensure your workflows call `bash scripts/run_orchestrator.sh` (recommended).
3) (Optional) run `python scripts/billing_state_hydrate.py --help` to see hydration options.

## Optional env vars
- `BILLING_STATE_DIR` (default: ./.billing-state)
- `BILLING_SCAFFOLD_DIR` (default: ./.billing-state-ci if present)
- `BILLING_RELEASE_TAG` (default: billing-state-v1)
- `BILLING_GH_REPO` (default: $GITHUB_REPOSITORY)
