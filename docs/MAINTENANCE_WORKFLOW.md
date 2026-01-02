# Maintenance Workflow

This repository uses a mandatory **Maintenance** workflow to keep the platform's repo-managed
registries and seeds consistent after every merge.

The key principle is separation of duties:

- **Maintenance** owns repository servicing and consistency (canonical IDs, registries, defaults).
- **Orchestrator** executes workorders only; it must assume the repo is already serviced.

## What Maintenance does

The workflow runs `scripts/maintenance_repo.py` (authoritative) and `platform.cli maintenance` to
generate and validate platform-maintained artifacts.

### 1) Canonicalize module folders (module IDs)
**Goal:** Ensure every module folder name is a canonical 6-digit ID (`NNNNNN`).

**Why:** Module IDs are used as stable keys for registries, pricing, schemas, dependency graphs, and
secrets prefixes.

**Output:** Modules are renamed/merged into `modules/<NNNNNN>/` and internal references are updated.

### 2) Canonicalize tenant folders (tenant IDs)
**Goal:** Ensure every tenant folder name is a canonical 10-digit ID (`NNNNNNNNNN`).

**Why:** Tenant IDs are accounting keys and must be stable across billing-state ledgers and runtime.

**Output:** Tenants are renamed/merged into `tenants/<NNNNNNNNNN>/` and `tenant.yml` is normalized.

### 3) Backfill module pricing rows
**Goal:** Ensure `platform/billing/module_prices.csv` contains an **effective active** price row for
every module folder.

**Why:** Spend estimation requires a price for each module. Prices are repo-managed configuration.

**Defaults:** Values come from `platform/billing/billing_config.yaml` (default run and artifact prices).

### 4) Regenerate platform registries
**Goal:** Keep platform registries in sync with the actual modules present.

**Outputs:**
- `platform/modules/modules.csv`
- `platform/modules/requirements.csv`
- `platform/errors/error_reasons.csv`
- `platform/schemas/work_order_modules/<module_id>.schema.json`

### 5) Sync billing-state tenant ledger (seed + release)
**Goal:** Ensure every repo tenant exists in the accounting ledger `tenants_credits.csv`, even if
their balance is `0`.

**Why:** Orchestrator hard-fails if a tenant is missing from `tenants_credits.csv`.

**Actions:**
- Updates the repo seed: `billing-state-seed/tenants_credits.csv`.
- If the GitHub Release `${BILLING_RELEASE_TAG}` exists, downloads its `tenants_credits.csv`,
  backfills missing tenants, and re-uploads the corrected asset.

## What Maintenance must NOT do
- It must not execute workorders.
- It must not produce runtime outputs.
- It must not change billing-state accounting tables beyond the declared servicing goals.

## Verification contract
After Maintenance finishes, `scripts/maintenance_repo.py --check` must report no changes.
E2E verification should run **after** Maintenance so that validation reflects the serviced state.
