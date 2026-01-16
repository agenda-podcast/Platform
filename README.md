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
python scripts/ci_verify.py --phase post
```

### 3) Offline publish guardrail (no-publish)

Run the publisher in *no-op* mode to validate it imports and executes end-to-end without requiring a runtime snapshot.

```bash
set -euo pipefail
rm -rf dist_artifacts_guardrail runtime-guardrail
mkdir -p runtime-guardrail
PLATFORM_OFFLINE=1 python scripts/publish_artifacts_release.py \
  --runtime-profile config/runtime_profile.dev_github.yml \
  --billing-state-dir billing-state-seed \
  --runtime-dir runtime-guardrail \
  --dist-dir dist_artifacts_guardrail \
  --since 2100-01-01T00:00:00Z \
  --no-publish
```

## Verification

This repository uses three verification workflows with progressively broader scope:

- **Verify Platform**: repository invariants only (no module execution).
- **Verify Modules**: executes a single module `testing.self_test`.
- **Verify Workorders**: executes a real platform-tenant workorder (`tenant_id=000000`).

Details, including dropdown generation, manual overrides, and cache cleanup policy:
- `docs/verification.md`

Local entrypoints:

```bash
set -euo pipefail
python scripts/verify_platform.py
python scripts/verify_module.py --module-id <module_id>
python scripts/verify_workorder.py --work-order-id <work_order_id>
```

## Admin top-up (development)

Admin top-up posts a TOPUP transaction to billing state.

```bash
python -m platform.cli admin-topup --tenant-id nxlkGI --amount-credits 1000 --topup-method-id bank-wire --reference wire-123 --note "dev seed top-up" --billing-state-dir .billing-state
```

## Canonical references

- `docs/verification.md` documents the verification workflows (Platform, Modules, Workorders), dropdown generation, manual overrides, and cache cleanup model.
- `docs/release_checklist.md` is the pre-merge operator checklist.
- `docs/schemas.md` defines canonical schemas and validation rules for module contracts and workorders.
