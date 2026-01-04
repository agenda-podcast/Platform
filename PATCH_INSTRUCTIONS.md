# Integrate Release Sync into Orchestrator (Patch Instructions)

This patch removes the need to run `scripts/release_sync.py` and **moves release syncing into orchestration**.

## What is included

- `platform/orchestration/release_sync.py`
  - Import-safe module that can be invoked from the orchestrator.
  - Uploads a ZIP of a workorder outputs directory to **GitHub Releases**.
  - Conditional behavior:
    - runs only if the workorder/tenant indicates purchase of `artifacts_download`, unless overridden.

- `scripts/release_sync.py`
  - Replaced with a **deprecation no-op** so legacy workflows no longer fail with import errors.

- `scripts/run_release_sync.sh`
  - Replaced with a **deprecation no-op** so legacy workflows do not fail.

- `scripts/e2e_assert_release_sync.py`
  - E2E check ensuring release sync is import-safe and does not crash without GitHub creds.

## Required orchestrator change (manual insert)

In `platform/orchestration/orchestrator.py`, after a workorder completes successfully (i.e., after outputs are written and status is set to COMPLETED), add:

```python
from platform.orchestration.release_sync import maybe_sync_artifacts_to_release

# ... after workorder success
sync_res = maybe_sync_artifacts_to_release(
    tenant_id=tenant_id,
    work_order_id=work_order_id,
    tenants_dir=Path(tenants_dir),
    runtime_dir=Path(runtime_dir),
    workorder_dict=workorder_dict,
)

if sync_res.ran:
    logger.info(
        "Release sync uploaded artifacts: tag=%s asset=%s",
        sync_res.tag,
        sync_res.asset_name,
    )
else:
    logger.info("Release sync skipped: %s", sync_res.skipped_reason)
```

If your orchestrator does not keep `workorder_dict` as a dict, pass whatever object you already have and adjust the purchase extraction logic accordingly. The module checks multiple common keys (`purchases`, `purchased_features`, `features`, etc.) and tolerates missing keys.

## Purchase flag semantics

Release sync runs when **any** of the following are true:

- `PLATFORM_FORCE_RELEASE_SYNC=1` (override)
- Workorder contains `purchases: ["artifacts_download"]` (case-insensitive)
- Workorder contains `features: {artifacts_download: true}`
- Tenant config file indicates the same (optional; if you wire it)

Disable globally:

- `PLATFORM_DISABLE_RELEASE_SYNC=1`

## E2E update (required)

Add this to your offline E2E job after orchestrator execution:

```bash
python scripts/e2e_assert_release_sync.py
```

## Notes

- GitHub Releases upload requires:
  - `GITHUB_TOKEN`
  - `GITHUB_REPOSITORY` (`owner/repo`)

If either is missing, release sync becomes a safe no-op.
