# ID Normalization & Deterministic Merge

## Problem
Leading zeros in numeric-looking identifiers (e.g., `0000000001`) are often stripped by CSV tooling (Excel, pandas, etc.). When your system later matches IDs as strings, joins fail (tenant credits not found, module/workorder mismatches, etc.).

## Policy (Canonical Matching Key)
Use the following canonicalization for **matching** and **lookup**:

- Trim whitespace
- If the value is **digits-only** (`^\d+$`), treat it as numeric and normalize by removing leading zeros:
  - `000001` => `1`
  - `000` => `0`
- Otherwise, keep the string as-is after trimming (supports future prefixed IDs like `t0001`, `wo-42`, `E001`).

The canonical function is implemented in:

- `platform/common/id_normalize.py` (`normalize_id`)

## Deterministic Merge
Some tables are keyed primarily by an ID (e.g., `tenants_credits.csv` by `tenant_id`). If a file contains duplicates that collapse under normalization (e.g., `1` and `0001`), the loader must resolve deterministically to avoid hard failures or double-counting.

Implementation:

- `dedupe_rows_by_normalized_id(..., prefer='latest')`
  - Keeps the row with the most recent timestamp across: `updated_at`, `modified_at`, `created_at`, `received_at`
  - If timestamps are absent, keeps the later row in the input file (stable)
  - Drops the other duplicates (conservative; avoids inflating balances)

## Where to apply
Apply normalization at **three** layers:

1. **Ingestion**: normalize ID fields immediately when loading CSV/YAML/JSON inputs.
2. **Matching**: normalize the key used in any dict/map lookup (or ensure everything was normalized at ingestion).
3. **Write-back**: write normalized IDs so the repo stays canonical.

Minimum touchpoints:

- Orchestrator: tenant_id, work_order_id, module_id matching
- Billing loaders: tenants_credits, transactions, payments, topup instructions
- Maintenance reconciliation: ledger joins using tenant/workorder IDs
- CI/E2E: add a scenario where billing has tenant `1` and workorder has `0000000001` and orchestration succeeds.

## One-time canonicalization
Use:

```bash
python -m platform.billing.normalize_billing_state --billing-state-dir .billing-state
```

This normalizes ID fields and dedupes key tables deterministically.
