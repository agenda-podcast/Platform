# Seed removal update

You removed:
  platform/billing_state_seed/

Therefore any workflow steps that reference:
  scripts/ci_verify_prices_seed.py
or:
  platform/billing_state_seed/module_prices.csv
must be removed.

Replacement verification:
  scripts/ci_verify_module_prices.py

Source of truth pricing config remains:
  platform/billing/module_prices.csv

Maintenance now:
- backfills missing module_id rows into platform/billing/module_prices.csv (using billing defaults)
- verifies coverage against modules/ folders
- commits only module_prices.csv (+ billing_config.yaml if present)
