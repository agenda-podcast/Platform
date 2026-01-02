# Maintenance Workflow

## Goal
The Maintenance Workflow is the platform’s authoritative repository servicing step. It runs after merges to keep the repository in a canonical state so that:

- Orchestration can execute Work Orders deterministically without mutating registries, module folders, or billing artifacts.
- Tenant configuration UI can be generated from deterministic JSON schemas.
- Billing configuration and tenant ledgers remain consistent with the current set of tenants and modules.

## When it runs
- Trigger: after every merge to the default branch (for example, `push` to `main`).
- Ordering: Maintenance must complete before E2E Verification runs.

## Authoritative implementation
Maintenance is implemented by the script:

- `scripts/maintenance_repo.py`

The workflow should call this script once per run and then commit any resulting repository changes.

## Servicing jobs performed
The script performs the following servicing jobs.

### 1) Canonicalize module folder naming
Ensures every module folder in `modules/` is named using the canonical module id format:

- `NNNNNN` (6 consequential digits)

If a legacy module folder exists (for example, `001`), it is renamed/canonicalized.

### 2) Canonicalize tenant folder naming
Ensures every tenant folder in `tenants/` is named using the canonical tenant id format:

- `NNNNNNNNNN` (10 consequential digits)

Legacy tenant folders (for example, `tenant-001`) are normalized.

### 3) Apply module id placeholder substitution
For newly added modules that were created before they had a final module id, the script replaces placeholders inside module files so that:

- environment variable prefixes are the module id
- internal names that must be unique are module-id-prefixed

### 4) Backfill module prices
Ensures `platform/billing/module_prices.csv` contains at least one active, effective price row for every module folder present in `modules/`.

Defaults are read from:

- `platform/billing/billing_config.yaml`

### 5) Regenerate platform registries and schemas
Regenerates platform registries that the UI and operations tooling rely on:

- `platform/modules/modules.csv`
- `platform/modules/requirements.csv`
- `platform/errors/error_reasons.csv`
- `platform/schemas/work_order_modules/<module_id>.schema.json` (synced from each module’s `tenant_params.schema.json` when present)

### 6) Ensure tenant credit ledgers include all tenants
Ensures every canonical tenant folder has a row in `tenants_credits.csv` (even if the balance is 0). This prevents orchestration from failing with:

- `Tenant not found in tenants_credits.csv`

The script updates the file in-place for any of the following ledger directories that exist in the repository workspace:

- `.billing-state/`
- `.billing-state-ci/`
- `billing-state-seed/`

If a ledger directory exists but `tenants_credits.csv` is missing, it will be created inside that directory.

## Validation behavior
The script supports a validation-only mode:

- `scripts/maintenance_repo.py --check`

In `--check` mode, the script computes the required changes but does not write them. If changes would be made, it exits non-zero so the workflow can fail.

## Responsibilities and non-responsibilities
Maintenance is responsible for repository servicing. Orchestration must not:

- rename module or tenant folders
- update module prices
- regenerate platform registries
- patch work orders for UI editing
- mutate billing-state ledgers except through normal run logging

If a platform behavior requires any of the above, that logic must be implemented in Maintenance and validated by E2E Verification.
