Your orchestrator is called with --billing-state-dir .billing-state-ci and fails:
  KeyError: Missing active module price for module 001

That indicates the orchestrator is reading prices from:
  .billing-state-ci/module_prices.csv
(not from platform/billing/module_prices.csv).

This patch updates Maintenance helper to backfill BOTH:
- platform/billing/module_prices.csv  (repo/config; CI header enforcement)
- <billing-state-dir>/module_prices.csv (runtime; orchestrate)

Use it in CI BEFORE orchestrate:

python scripts/maintenance_modules.py \
  --modules-dir modules \
  --billing-state-dir .billing-state-ci \
  --prices-path platform/billing/module_prices.csv \
  --report-path runtime/maintenance_modules_report.json

Then optionally confirm:
python scripts/debug_billing_state_prices.py --billing-state-dir .billing-state-ci --module-id 001

After this, orchestrate should no longer fail on missing module 001 price.

Also update E2E verification to assert:
- billing-state module_prices.csv exists
- header matches expected schema
- every module_id referenced by the workorder has an effective+active price row in billing-state
