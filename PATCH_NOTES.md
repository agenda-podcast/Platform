# PLATFORM patch v2 — release sync workflow fixes

This patch addresses two failures seen in GitHub Actions:

1) **`Error: Unable to resolve action cli/cli@v2`**
- `cli/cli@v2` is **not** a valid GitHub Action reference.
- GitHub-hosted runners already include the **`gh`** CLI, so the workflow now **verifies `gh` exists** and uses it directly.

2) **`main -> main (fetch first)` push rejection**
- This happens when the remote branch advanced (e.g., another workflow run committed) after your checkout.
- The workflow now:
  - checks out with **full history** (`fetch-depth: 0`) so rebase is possible
  - **fetches + rebases** on `origin/<branch>` before committing
  - **retries push** up to 3 times with rebase in between

## Files included
- `.github/workflows/sync-releases.yml`
- `scripts/` (Base62 IDs, internal release-alias mapping, release sync logic)

## Notes
- Billing release folder remains fixed: `releases/billing-state-v1/`
- Mapping table is stored at: `.platform/internal/release_id_map.csv`

