This patch fixes the persistent runtime error:

  KeyError: Missing active module price for module 001

Root cause:
- Orchestrator is currently loading module_prices from runtime/billing-state artifacts.
- Your policy now makes pricing a platform-maintained config at:
    platform/billing/module_prices.csv
- When billing-state lacks prices, spend estimation fails.

What this patch does:
- Keeps existing behavior for already-loaded module_prices.
- Adds a strict fallback in _find_price() to load authoritative pricing from:
    platform/billing/module_prices.csv
- Normalizes module_id to 3-digit format (001) to avoid lookup mismatches.
- Selects the best effective+active row (latest effective_from).

How to apply:
1) Apply patch file:
   patches/orchestrator_repo_prices_fallback.patch
   to platform/orchestration/orchestrator.py

2) Ensure Maintenance backfills module_prices.csv:
   python scripts/maintenance_prices.py --modules-dir modules --module-prices-path platform/billing/module_prices.csv --billing-config-path platform/billing/billing_config.yaml

Notes:
- This does NOT make orchestrator "do maintenance". It only reads the authoritative platform pricing config for spend estimation.
- After this change, orchestrate will no longer depend on billing-state containing module_prices.csv.
