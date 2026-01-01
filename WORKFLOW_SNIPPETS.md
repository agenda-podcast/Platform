## Mandatory CI job order (this is why your orchestrate still fails)

Your orchestrate job currently runs without first generating/refreshing billing-state price files.
Because orchestrate reads *billing-state* prices, it will continue to fail until you backfill prices in
the same job, immediately before orchestrate.

### Add these steps to the Orchestrate job (same job)

```yaml
- name: Backfill module prices into billing-state
  run: |
    python scripts/maintenance_prices_backfill.py       --modules-dir modules       --billing-state-dir .billing-state-ci       --skip-repo-write       --default-run-credits 5       --default-save-to-release-credits 2

- name: Verify billing-state module prices
  run: |
    python scripts/verify_billing_state_prices.py       --modules-dir modules       --billing-state-dir .billing-state-ci

- name: Orchestrate
  run: |
    python -m platform.cli orchestrate       --runtime-dir runtime       --billing-state-dir .billing-state-ci
```

### Add this step to Maintenance workflow (repo table)
This keeps the repo-config table in sync and makes CI pre-phase checks deterministic.

```yaml
- name: Backfill module prices in repo config
  run: |
    python scripts/maintenance_prices_backfill.py       --modules-dir modules       --repo-prices-path platform/billing/module_prices.csv       --default-run-credits 5       --default-save-to-release-credits 2
```

### Why this fixes module 001
If module 001 is missing from the billing-state price table (or is inactive / future-dated), the backfill script:
- adds it with (5, 2)
- forces effective_from <= today and active=true
- writes the file(s) under billing-state so orchestrate can load them
