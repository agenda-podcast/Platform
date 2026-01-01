You are still getting:
  KeyError: 'Missing active module price for module 001'

That means platform/orchestration/orchestrator.py still has the old _find_price() implementation.

This bundle provides a deterministic patcher script that rewrites the *top-level* function:

  def _find_price(...)

inside:
  platform/orchestration/orchestrator.py

to a version that:
- first tries the runtime-loaded price table (module_prices param)
- then falls back to the authoritative config:
    platform/billing/module_prices.csv
- normalizes numeric module ids to 3 digits (001)
- selects an effective+active row

Run once locally or in a Maintenance workflow step, then commit the changed orchestrator.py.

Usage:
  python scripts/patch_orchestrator_find_price.py --backup

After patching, re-run:
  python -m platform.cli orchestrate --runtime-dir runtime --billing-state-dir .billing-state-ci

Expected behavior:
- No KeyError for module 001 if platform/billing/module_prices.csv contains an effective+active row for 001.
- If runtime table is missing, you will see:
    [ORCH][WARN] ... used repo pricing config instead.
