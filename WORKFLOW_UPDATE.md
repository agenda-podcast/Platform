# Maintenance Workflow update (pricing backfill)

## Goal
Ensure that when a new module folder is added (e.g., `003_google_search_pages`), the platform always has a price row for that module.
If a module is not mentioned in the price table, add it with defaults:

- `price_run_credits = 5`
- `price_save_to_release_credits = 2`

## Add this step to Maintenance workflow (before any CI verification that reads module_prices.csv)

```yaml
- name: Backfill module prices (repo)
  run: |
    python scripts/maintenance_prices_backfill.py       --modules-dir modules       --repo-prices-path platform/billing/module_prices.csv       --default-run-credits 5       --default-save-to-release-credits 2
```

Then commit the updated file (typical pattern):

```yaml
- name: Commit maintenance outputs
  run: |
    git status --porcelain
    git add platform/billing/module_prices.csv
    git commit -m "Maintenance: backfill module prices" || echo "No changes"
    git push
```

## Add this step to Orchestrate job (runtime billing-state)
Because orchestrate reads prices from `--billing-state-dir`, also ensure billing-state has the price table:

```yaml
- name: Backfill module prices (billing-state)
  run: |
    python scripts/maintenance_prices_backfill.py       --modules-dir modules       --billing-state-dir .billing-state-ci       --skip-repo-write       --default-run-credits 5       --default-save-to-release-credits 2
```

This prevents:
`KeyError: Missing active module price for module 001`
