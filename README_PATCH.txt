PLATFORM Billing Fresh-Start Bootstrap Patch

This package adds platform/billing/bootstrap.py and provides patch diffs
to wire it into:
- platform/orchestration/orchestrator.py (wrap validate_minimal)
- platform/billing/maintenance.py (ensure bootstrap at Maintenance start)

Rationale:
- If you delete the GitHub Release assets for billing-state-v1 or start fresh,
  CI and orchestrator will fail early.
- This patch makes the system self-healing:
  * bootstraps local billing-state directory from repo template
  * republishes missing assets to GitHub Releases (best-effort)

FILES INCLUDED:
- platform/billing/bootstrap.py (NEW)
- PATCH_orchestrator.diff
- PATCH_maintenance.diff

You must apply the *.diff patches to your repo files (they are small, safe edits).
