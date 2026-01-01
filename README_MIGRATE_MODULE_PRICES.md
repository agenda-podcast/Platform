This patch addresses CI pre-phase failure:

[CI_VERIFY][FAIL] CSV header mismatch: platform/billing/module_prices.csv

Your repo currently has a LEGACY header:
  ['module_id','price_unit','price_credits','price_scope','note']

But CI expects:
  ['module_id','price_run_credits','price_save_to_release_credits','effective_from','effective_to','active','notes']

Action:
1) Run migration:
   python scripts/migrate_module_prices_csv.py --path platform/billing/module_prices.csv

2) Commit the rewritten platform/billing/module_prices.csv to the repo.

Notes:
- The migration maps legacy price_credits -> price_run_credits.
- price_save_to_release_credits is set to 0.
- effective_from defaults to 2026-01-01 (adjust if you maintain historical pricing).
- active is set to true.
- legacy pricing semantics are preserved in notes (legacy_unit, legacy_scope, note).
