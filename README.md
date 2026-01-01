Fix for: KeyError 'Missing active module price for module 001'

Root causes (either one, often both):
1) module_prices.csv was migrated/rewritten without a row for module 001 (or other existing modules).
2) effective_from was set to a future date, so no "active price" is effective at runtime.

This patch provides:
- scripts/maintenance_modules.py: backfills module_prices.csv for ALL modules under ./modules
  using module.yaml pricing (or safe defaults) and ensures an effective+active row exists.
- scripts/migrate_module_prices_csv.py: migrates legacy header to expected header with
  effective_from=1970-01-01 to avoid "future price" issues.

Recommended workflow:
A) If module_prices.csv still has legacy header:
   python scripts/migrate_module_prices_csv.py --path platform/billing/module_prices.csv
   git add platform/billing/module_prices.csv && git commit -m "Migrate module_prices.csv schema"

B) Run Maintenance (this helper) in your Maintenance workflow before orchestrate:
   python scripts/maintenance_modules.py --modules-dir modules --report-path runtime/maintenance_modules_report.json

C) Re-run orchestrate. Module 001 will have an active price.
