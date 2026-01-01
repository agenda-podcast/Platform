# Maintenance workflow: add this step BEFORE any commit/push step

- name: Maintenance - backfill module prices
  run: |
    set -euxo pipefail
    python scripts/maintenance_prices.py       --modules-dir modules       --module-prices-path platform/billing/module_prices.csv       --billing-config-path platform/billing/billing_config.yaml

# Then commit ONLY platform/billing/module_prices.csv (+ your other maintenance outputs)
