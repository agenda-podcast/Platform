# Verification scripts

This folder contains repository entrypoint scripts used by GitHub Actions verification workflows.

Authoritative documentation:
- `docs/verification.md`

## Scripts

- `verify_platform.py`
  - Scope: repository invariants only (no module execution, no workorder execution).
  - Used by: `.github/workflows/verify_platform.yml`.

- `verify_module.py`
  - Scope: execute one module `testing.self_test` defined in `modules/<module_id>/module.yml`.
  - Used by: `.github/workflows/verify_modules.yml`.

- `verify_workorder.py`
  - Scope: execute one platform-tenant workorder end-to-end (`tenant_id=00000t`).
  - Optional helper for local single-workorder runs; the canonical CI path is `.github/workflows/orchestrator.yml`.

- `maintenance_regen_verify_dropdowns.py`
  - Scope: regenerate workflow dropdown lists from Maintenance-generated indexes.
  - Inputs (only):
    - `maintenance-state/modules_index.csv`
    - `maintenance-state/workorders_index.csv`
  - Outputs (only):
    - rewrites delimited blocks in `.github/workflows/verify_modules.yml` and `.github/workflows/orchestrator.yml`.

## Local usage

```bash
set -euo pipefail
python scripts/verify_platform.py
python scripts/verify_module.py --module-id deliver_email
python scripts/verify_workorder.py --work-order-id PlatEm01
```
