# PLATFORM (Modular, Release-backed State)

This repository implements a small, modular “platform” runner with:

- **Modules** (in `modules/<module_id>/`) executed by the orchestrator
- **Tenants + Work Orders** (in `tenants/<tenant_id>/workorders/`)
- **Release-backed billing state** (GitHub Release tag is the **system of record**)
- **Maintenance** that regenerates derived indexes in `maintenance-state/`
- **E2E verification** that enforces schemas, headers, and ID policy
- A **standalone Cache Prune** workflow (left unchanged)

## ID policy (Base62, fixed length, randomized, de-duplicated)

All IDs are **random Base62** using the alphabet: `0-9 A-Z a-z`.

| Entity | Length |
|---|---:|
| Tenant ID | 6 |
| Work Order ID | 8 |
| Module ID | 3 |
| Transaction ID | 8 |
| Transaction Item ID | 8 |
| Module Run ID | 8 |
| Reason Code | 6 |
| Reason Key | 3 |
| Payment ID | 8 |
| Top-up Method ID | 2 |
| Product Code | 3 |
| GitHub Release / Asset ID (internal) | 8 |

Validation and generation are implemented in:
- `platform/common/id_policy.py`
- `platform/common/id_codec.py`

## Release-backed billing state (fixed tag)

Billing state is **not stored as editable source of truth in the repository**.

- The **source-of-truth release tag is fixed**: `billing-state-v1`
- Workflows download the release assets into `.billing-state/`, mutate locally, then upload back.

Billing-state assets (CSV) include:
- `tenants_credits.csv`
- `transactions.csv`
- `transaction_items.csv`
- `promotion_redemptions.csv`
- `cache_index.csv`
- `workorders_log.csv`
- `module_runs_log.csv`
- `github_releases_map.csv` (internal release_id -> GitHub numeric release id)
- `github_assets_map.csv` (internal asset_id -> GitHub numeric asset id)
- `state_manifest.json`

Seeds for a fresh release are in `billing-state-seed/`.

## GitHub Release/Asset internal mapping (anti-enumeration)

For module artifacts published to GitHub Releases, the platform uses an **internal** random 8-char ID
(`github_release_asset_id`) as the “release_id” and “asset_id”. The numeric GitHub IDs are stored in billing state:

- `platform/billing-state/github_releases_map.csv`
- `platform/billing-state/github_assets_map.csv`

This enables internal folder naming and avoids exposing sequential GitHub IDs.

## Key commands

- Maintenance (regenerates `maintenance-state/`):
  ```bash
  python -m platform.cli maintenance
  ```

- Orchestrator (runs enabled work orders):
  ```bash
  python -m platform.cli orchestrator --billing-state-dir .billing-state --runtime-dir runtime --enable-github-releases
  ```

- Admin top-up (posts a TOPUP transaction):
  ```bash
  python -m platform.cli admin-topup --billing-state-dir .billing-state --tenant-id <TENANT> --topup-method-id <TM> --amount-credits 1000 --reference "wire-123"
  ```

## Workflows

- **maintenance.yml**: regenerates maintenance-state + verifies repo
- **orchestrator.yml**: runs work orders and updates the billing-state release assets
- **admin-topup.yml**: applies a top-up and updates the billing-state release assets
- **verify-e2e.yml**: runs an end-to-end pass of maintenance + orchestrator + verification
- **cache-prune.yml**: standalone cache cleanup (unchanged)

