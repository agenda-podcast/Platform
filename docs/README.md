This patch fixes the pipeline ordering issue and clarifies how --check is intended to be used.

Key points:
- `python scripts/maintenance_repo.py --check` is a *verification* mode. It MUST fail if legacy+canonical folders both exist
  with differing files, because the repo is not canonical. That is expected.

- Post-merge automation must run Maintenance in APPLY mode first:
    python scripts/maintenance_repo.py

  which converges the repo (merges/renames) and then a subsequent:
    python scripts/maintenance_repo.py --check
  should pass.

- GitHub Actions ordering:
  - Maintenance runs on every push to main (i.e., after merges).
  - E2E Verification runs after Maintenance (as a dependent job), and checks idempotency again.

- Loop prevention:
  - The maintenance job does not run for github-actions[bot] commits (the commit it pushes).
