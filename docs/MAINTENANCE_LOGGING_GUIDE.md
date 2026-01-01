# Why your Maintenance workflow can “fail without logs” and how to force logs

In GitHub Actions, “no logs” usually means one of these happened:

1) **Workflow file failed to parse** (YAML syntax / indentation / invalid keys)
   - You will see an error banner on the workflow run page (before any steps start).
   - Fix: validate YAML and ensure the file is in `.github/workflows/*.yml`.

2) **Job never started** (repo Actions disabled, permissions blocked, concurrency cancelled)
   - Check repo Settings → Actions, and the run summary “This workflow was disabled”.

3) **Script step exits immediately and you never printed anything**
   - Fix: run shell with `set -euxo pipefail`, and add an `if: always()` dump step.

This patch updates your Maintenance workflow to:
- run all `run:` blocks under `set -euxo pipefail`
- print environment + dependency state early
- dump artifacts even on failure (`if: always()`)
- upload artifacts so you can inspect `runtime/maintenance_prices_report.json` and the generated CSVs

## Also enable GitHub step debugging (optional)
Add a repository secret:
- `ACTIONS_STEP_DEBUG = true`

This makes Actions print more runner-level details.
