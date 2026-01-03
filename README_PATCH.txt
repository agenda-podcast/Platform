PATCH: Fresh-start workflow (removes cli/cli@v2)

What to do:
1) Find the workflow file in your repo that currently contains:
   uses: cli/cli@v2
2) Delete that step entirely, or replace the whole workflow with:
   .github/workflows/sync_releases.yml from this patch zip.

Why:
- `cli/cli@v2` is NOT a valid GitHub Action ref, so Actions fails during "Getting action download info".
- GitHub-hosted runners already ship with the `gh` CLI, so you can call `gh ...` directly.

Also included:
- A push-safe pattern: fetch + rebase before commit, and retry push up to 3 times to avoid
  non-fast-forward failures when main advances during the run.
