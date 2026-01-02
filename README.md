You are still seeing:
  KeyError: Missing active module price for module 000001

This means the orchestrator still cannot find an effective+active row for module_id == "001"
in the *prices list it loaded*.

Two common root causes:
1) The prices file used at runtime is not the one you updated (path mismatch under billing-state-dir).
2) module_id in CSV is not zero-padded (e.g., "1" instead of "001"), so lookup fails.

This patch adds:
- scripts/maintenance_prices.py
  - Ensures platform/billing/module_prices.csv has rows for ALL module folders (001, 002, ...)
  - If --billing-state-dir is provided, writes module_prices.csv to MULTIPLE plausible locations:
    - <billing-state>/module_prices.csv
    - <billing-state>/billing/module_prices.csv
    - <billing-state>/platform/billing/module_prices.csv
  - Normalizes existing module_id values by zero-padding digits to 3 chars.

- scripts/debug_find_module_prices.py
  - Recursively finds all module_prices.csv under billing-state-dir and reports:
    - header status
    - whether module 000001 exists
    - whether it is effective+active
    - warns if ID is present but not zero-padded.

CI usage (before orchestrate):
  python scripts/maintenance_prices.py --modules-dir modules --billing-state-dir .billing-state-ci
  python scripts/debug_find_module_prices.py --billing-state-dir .billing-state-ci --module-id 001

If debug shows module_id is "1" (not "001"), then this patch will fix it by rewriting the file(s).

Next (recommended):
- Update scripts/ci_verify.py PRE phase to run maintenance_prices.py and to fail if no EFFECTIVE_ACTIVE price
  exists for any module id referenced by the workorder.
