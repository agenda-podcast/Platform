# PLATFORM (Modular runner with release-backed state)

PLATFORM is a modular runner that executes tenant work orders, records outputs, and maintains a release-backed billing state suitable for development and verification workflows.

## What this repository contains

- **Modules** in `modules/<module_id>/` with explicit contracts in `module.yml` (inputs, outputs, deliverables, and `kind`).
- **Tenants and work orders** in `tenants/<tenant_id>/workorders/*.yml`.
- **Billing state (append-only logs)** under `.billing-state*/` and published as GitHub Release assets in development mode.
- **Maintenance** that regenerates derived indexes under `maintenance-state/` and validates repository invariants.
- **Packaging and delivery** modules that turn outputs into downloadable artifacts when the tenant requests them.

## Separation of concerns (critical)

### Orchestrator (execution)

`python -m platform.cli orchestrator` executes enabled work orders:

- Loads the work order YAML
- Executes steps in order
- Writes step run records and output records into the runtime directory
- Posts billing ledger items (with deterministic idempotency keys)
- Computes final run status (COMPLETED or PARTIAL) based on step outcomes and delivery policy

The orchestrator does not publish artifacts to GitHub Releases.

### Publisher script (workflow-driven publish)

`scripts/publish_artifacts_release.py` is invoked by workflows as a separate step:

- Scans the billing state to identify purchased deliverables
- Packages and publishes those deliverables (or simulates publishing with `--no-publish`)
- Skips internal deliverable IDs (for example `__run__`, `__delivery_evidence__`, and any deliverable starting with `__`)
- Writes publish evidence records used by verification scripts

## Artifact delivery policy (enforced)

These rules are intentionally strict for enabled work orders and intentionally permissive for drafts.

1) **Activation gating**
- If `enabled: true` and `artifacts_requested: true`, the work order must include:
  - At least one step with `kind: packaging`
  - At least one step with `kind: delivery`
- If any enabled work order includes a `kind: packaging` step (regardless of `artifacts_requested`), it must also include at least one `kind: delivery` step.

2) **No auto-injection**
- The platform does not inject packaging or delivery steps into work orders.
- Validation blocks activation for enabled work orders that violate the policy.
- Draft work orders are allowed to save with warnings.

3) **Email size cap**
- Email delivery enforces a hard cap of 19.9 MB.
- When a package exceeds the cap, the delivery step fails with reason `package_too_large_for_email` and (when eligible) a refund is posted for the delivery run.

4) **Publisher internal deliverables**
- Publishing scans ignore internal deliverable IDs to prevent accidental publication of platform-internal artifacts.

## Dev GitHub Releases versus tenant-owned delivery

- **Development mode (`runtime_profile.dev_github.yml`)** uses GitHub Releases as a system-of-record for billing state assets and for development and test artifact storage.
- **Tenant-owned delivery** is the direction for production: tenants connect their own storage providers (for example Dropbox, S3, OneDrive, Google Drive) and deliveries are performed into tenant-owned accounts using tenant-scoped credentials.

The repository includes scaffolding for this direction (credentials store, OAuth callback service, and integration secret blocks), but it is not a production deployment guide.

## Quickstart (local)

### 1) Run unit tests

```bash
python -m pip install -r requirements.txt
pytest -q
```

### 2) Run the CI verifier locally

```bash
python scripts/ci_verify.py --phase pre
python scripts/ci_verify.py --phase e2e
```

### 3) Run the offline E2E parity sequence (matches `.github/workflows/e2e-verify.yml`)

The orchestrator runs all work orders that are `enabled: true`. The example below assumes tenant `nxlkGI` work order `UbjkpxZO` is enabled (it is enabled in this repository).

```bash
python -m platform.cli consistency-validate
python -m platform.cli integrity-validate

python -m platform.cli orchestrator --runtime-profile config/runtime_profile.dev_github.yml --billing-state-dir .billing-state-e2e --runtime-dir runtime-e2e
python -m platform.cli orchestrator --runtime-profile config/runtime_profile.dev_github.yml --billing-state-dir .billing-state-e2e --runtime-dir runtime-e2e

python scripts/publish_artifacts_release.py --runtime-profile config/runtime_profile.dev_github.yml --billing-state-dir .billing-state-e2e --runtime-dir runtime-e2e --since "$(cat .since_ts)" --no-publish

python scripts/e2e_assert_chaining.py --runtime-dir runtime-e2e --tenant-id nxlkGI --work-order-id UbjkpxZO
python scripts/e2e_assert_idempotency.py --billing-state-dir .billing-state-e2e --tenant-id nxlkGI --work-order-id UbjkpxZO
```

## Admin top-up (development)

Admin top-up posts a TOPUP transaction to billing state.

```bash
python -m platform.cli admin-topup --tenant-id nxlkGI --amount-credits 1000 --topup-method-id bank-wire --reference wire-123 --note "dev seed top-up" --billing-state-dir .billing-state
```

## Canonical references

- `docs/release_checklist.md` is the canonical operator checklist and verification posture.
- `docs/schemas.md` is the canonical schema and validation reference for work orders and module contracts.
