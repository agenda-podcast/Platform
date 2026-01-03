# PLATFORM Patch — Tenant Not Found Error Continuation (Release Sync + Base62 IDs)

This patch addresses the failure:

- `Error: Unable to resolve action \`cli/cli@v2\`, unable to find version \`v2\``

Root cause: **`cli/cli` is not a GitHub Action that can be referenced as `uses: cli/cli@v2`**.
GitHub-hosted runners already include the GitHub CLI (`gh`), and GitHub recommends using it
directly in workflows (with `GH_TOKEN`). See GitHub docs for using `gh` in workflows.

## What is included

1. `scripts/base62.py`  
   Cryptographically-random Base62 generator (`0-9,A-Z,a-z`).

2. `scripts/id_manager.py`  
   - Generates fixed-length IDs with persistent deduplication  
   - Stores used IDs in `.platform/internal/id_registry.json`
   - Maintains the internal release mapping table:
     `.platform/internal/release_id_map.csv`  
     Mapping: **8-char Base62 alias** → **GitHub numeric release id** (+ tag)

3. `scripts/release_sync.py`  
   - Syncs GitHub releases to `/releases/` folders
   - Billing release tag is **fixed** to: `billing-state-v1` (folder `releases/billing-state-v1/`)
   - All other releases are stored under `releases/<release_alias_id>/`

4. `.github/workflows/sync-releases.yml`  
   - Removes the invalid `cli/cli@v2` reference
   - Uses preinstalled `gh` and runs `python -m scripts.release_sync`

## If you run in a container job or self-hosted runner without gh

Use a marketplace action that installs gh (example):

- `actions4gh/setup-gh@v1` (third-party) — see GitHub Marketplace listing
  "Setup the GitHub CLI".

Or install gh via apt (Ubuntu).

## How to apply

Unzip into repo root (preserving paths), commit, and run workflow **Sync Releases Into Repo**.
