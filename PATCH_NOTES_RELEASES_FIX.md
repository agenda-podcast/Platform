# PLATFORM – Releases Publishing Fix

This patch addresses two issues observed in GitHub Actions runs:

1. **Billing-state Release not updating** even though workflows succeed.
2. **Module artifacts not appearing in Releases** even when the workorder requests / purchases artifact download.

## Root cause (high-level)

- A previously added **deprecated** `sync-releases` workflow/job can succeed without actually uploading anything.
- The orchestrator run path can be **offline by default** (or not auto-enabled) so release publishing never executes.
- In some runs, purchase flags are present under different keys; publication was gated on a narrower key (`purchase_release_artifacts`).

## What this patch changes

- `scripts/run_orchestrator.sh`
  - Hydrates billing-state locally from `billing-state-v1` Release first; falls back to `.billing-state-ci` seed.
  - Scans workorders for **any** artifact-download purchase signal and auto-passes `--enable-github-releases`.
  - Publishes the updated billing-state back to the fixed Release tag `billing-state-v1`.

- `scripts/billing_state_publish.py`
  - Uploads required CSV assets (and `state_manifest.json` if present) to the `billing-state-v1` Release.

- Adds a CI guard `scripts/artifacts_release_guard.py` (and optional e2e assertion) that fails the run if artifacts were purchased but are not visible in Releases.

- Updates `.github/workflows/orchestrator.yml` to:
  - Provide both `GITHUB_TOKEN` and `GH_TOKEN`
  - Remove repo-commit behavior for billing-state

## Manual cleanup recommended

- Delete any deprecated release-sync workflows or scripts that only print “deprecated” and do not upload assets.
  - Example: `.github/workflows/sync-releases.yml` (if it exists and is deprecated).

