# Runtime Evidence Publishing (Patch)

This patch updates `scripts/billing_state_publish.py` to also attach **`.billing-state/runtime_evidence_zips/*`** to the `billing-state-v1` GitHub Release.

## Usage

The workflow already calls:
```
python scripts/billing_state_publish.py
```

No changes to the workflow are required. On the next run, you should see the **runtime evidence zip and its manifest** in the `billing-state-v1` Release Assets.

## Expected Assets

- `runtime_evidence__tenant=<TENANT>__workorder=<WO>__<STAMP>.zip`
- `runtime_evidence__tenant=<TENANT>__workorder=<WO>__<STAMP>.manifest.json`

These provide auditable proof of per-step runtime outputs.
