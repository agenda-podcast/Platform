# E2E Verification requirement (pricing)

Add a pre-orchestrate verification that fails if any module referenced by the Work Order lacks an active/effective price row.

Minimum checks:
1) `.billing-state-ci/**/module_prices.csv` exists (at least one)
2) header equals:
   module_id,price_run_credits,price_save_to_release_credits,effective_from,effective_to,active,notes
3) For each module id:
   - a row exists
   - active == true
   - effective_from <= today
   - effective_to is empty or >= today

Recommendation:
- Run `maintenance_prices_backfill.py --billing-state-dir ...` before orchestrate.
- Then verify pricing table(s).
