# Modular GitHub Actions Platform (Multi-Tenant, Work Orders, Billing, Releases)

This repository implements a multi-tenant, fully modular execution platform built on **GitHub Actions** + **GitHub Releases**, designed for “pipeline-like” workloads (content generation, media processing, ETL-style jobs, etc.) with deterministic billing, artifact publishing, caching, and strict tenant isolation.

The platform is centered on:

- **Tenants** (separate configuration + optional local reusable assets)
- **Work Orders** (a tenant’s requested run plan: modules + parameters + reuse mode + artifact purchase)
- **Modules** (self-contained runnable units; each module lives in its own folder)
- **Maintenance State** (compiled indexes committed to the repo for fast runtime)
- **Billing State** (financial truth stored in a fixed Release tag with CSV tables)

---

## Table of Contents

- [Key Concepts](#key-concepts)
- [Repository Layout](#repository-layout)
- [Quick Start](#quick-start)
- [How to Run](#how-to-run)
- [Work Orders](#work-orders)
- [Modules](#modules)
- [Artifacts and Releases](#artifacts-and-releases)
- [Caching](#caching)
- [Billing](#billing)
- [Reason Codes and Policy](#reason-codes-and-policy)
- [Tenant Isolation and Cross-Tenant Reuse](#tenant-isolation-and-cross-tenant-reuse)
- [Workflows](#workflows)
- [Administrator Operations](#administrator-operations)
- [Docs](#docs)

---

## Key Concepts

### Tenant
A tenant is a logical customer/account. Tenants live under `tenants/<tenant_id>/` and include:

- `tenant.yml` (profile + release sharing policy)
- `workorders/` (requested executions)
- `assets/outputs/` (optional local reusable outputs library)

### Work Order
A Work Order is a YAML file under `tenants/<tenant_id>/workorders/` that:

- selects **module IDs** to run
- provides parameters for each module
- sets reuse mode (`new | cache | release | assets`)
- indicates whether the tenant is purchasing downloadable artifacts (per module)
- chooses completion mode (`STRICT` vs `PARTIAL_ALLOWED`)
- optionally supplies promo codes (discounts are modeled as separate negative line items)

### Module
Modules are runnable units in `modules/<module_id>/`.

**The module folder name _is_ the Module ID and is used everywhere.**

- configuration keys
- dependencies and ordering
- billing and pricing
- logs
- artifact naming and manifests

**Module ID requirement:** a three-digit numeric string `001–999` (e.g., `007`).

### Maintenance State
Maintenance produces compiled CSV tables committed to the repo under `maintenance-state/`.
These tables allow Work Order runs to be fast: no expensive scanning of tenants/modules at runtime.

### Billing State
Billing state is stored as CSV assets in a fixed GitHub Release tag: **`billing-state`**.
It includes credit balances, ledger tables, promos, cache index, and minimal operational logs.

---

## Repository Layout

```
.github/workflows/
  maintenance.yml
  orchestrator.yml
  run-module.yml
  cache-prune.yml

config/
  global_reasons.yml

maintenance-state/
  ids/
    category_registry.csv
    module_registry.csv
    reason_registry.csv
  reason_catalog.csv
  reason_policy.csv
  module_dependency_index.csv
  module_requirements_index.csv
  module_artifacts_policy.csv
  tenant_relationships.csv
  maintenance_manifest.csv

modules/
  <module_id>/
    module.yml
    validation.yml
    action.yml
    src/
    assets/
    schemas/
    README.md

tenants/
  <tenant_id>/
    tenant.yml
    assets/outputs/<folder_name>/(manifest.json + files)
    workorders/<work_order_id>.yml

runtime/  (ephemeral; ignored)
docs/
  architecture.md
  schemas.md
```

---

## Quick Start

### 1) Add at least one module
Create `modules/001/` with:

- `module.yml`
- `validation.yml`
- `action.yml`
- `src/...`

### 2) Register the module in maintenance registry
Add a row to:

- `maintenance-state/ids/module_registry.csv`

Example row:
- `module_id=001`
- `category_id=04` (Scripting), etc.

### 3) Create a tenant and a work order
Create:

- `tenants/tenant-001/tenant.yml`
- `tenants/tenant-001/workorders/wo-001.yml`

### 4) Run Maintenance
Maintenance compiles:

- dependencies index
- reason catalog + policy scaffolding
- tenant relationships
- requirements index
- platform artifact policy scaffolding (default allow if absent)

### 5) Bootstrap the `billing-state` Release
Create the `billing-state` release/tag and upload initial CSV tables (or run a bootstrap workflow once).

### 6) Run Orchestrator
Orchestrator discovers enabled work orders, checks credits, records Spend, runs modules, refunds where applicable, updates billing tables, and publishes purchased artifacts.

---

## How to Run

### Run all enabled work orders
Trigger `orchestrator.yml` (scheduled or manual).

### Run a single tenant / single work order
Recommended approach:
- set `enabled: true` only for the desired work order, and/or
- implement workflow inputs (e.g., `tenant_id`, `work_order_id` filters) in `orchestrator.yml`.

### Run one module individually
Best practice: create a work order containing exactly one module entry. This preserves billing, logs, artifacts, and deterministic behavior.

---

## Work Orders

Work order file: `tenants/<tenant_id>/workorders/<work_order_id>.yml`

Required fields:

- `work_order_id`
- `enabled: true|false`
- `mode: STRICT|PARTIAL_ALLOWED`
- `modules: [...]`

Each module entry includes:

- `module_id: "007"`
- `params: { ... }`
- `reuse_output_type: new|cache|release|assets`
- `release_tag` (required if reuse is `release`)
- `assets_folder_name` (required if reuse is `assets`)
- `cache_retention_override` (optional if reuse is `cache`)
- `purchase_release_artifacts: true|false`

Completion mode:

- `STRICT`: any **FAILED** module => Work Order **FAILED**
- `PARTIAL_ALLOWED`: Work Order can be **PARTIALLY_COMPLETED** if at least one module succeeds

---

## Modules

Each module is self-contained.

### `modules/<module_id>/module.yml`
Defines:

- capability flags (including `supports_downloadable_artifacts`)
- caching policy (enabled + retention default)
- dependency list (`depends_on` is nullable)
- input/output contract (names + formats)
- module version (used in cache keys and traceability)

### `modules/<module_id>/validation.yml`
Defines module-specific reason keys (Maintenance assigns numeric reason codes).

### `modules/<module_id>/action.yml`
Composite action invoked by the platform runner. It must:

- read resolved inputs from standardized locations/env
- run module logic
- write outputs to standardized location
- write a module outputs manifest (internal; even if not published)
- on failure, emit a numeric `reason_code` (from the compiled reason catalog)

---

## Artifacts and Releases

### Artifact eligibility is a 3-level gate
Artifacts are published only if all are true:

1) Module supports artifacts (`supports_downloadable_artifacts=true`)
2) Platform enables artifacts for the module (default enabled; admin can disable via `maintenance-state/module_artifacts_policy.csv`)
3) Tenant purchases artifacts for that module (`purchase_release_artifacts=true`)

If tenant purchases artifacts but artifacts are not eligible, the module run **must fail** (it can be meaningless to run otherwise).

### Work order release
Artifacts are published to a GitHub Release with tag:

- `wo-<tenant_id>-<work_order_id>`

### File naming convention
All published files:

- `tenantid-work_orderid-moduleid-itemid-shorthash.extension`
- lowercase, no spaces, max length enforced

### One manifest per work order release
The release must include:

- `tenantid-work_orderid-manifest.json`

This manifest includes:

- `owning_tenant_id`
- `work_order_id`
- list of items (filename, module_id, hashes, sizes, etc.)

The manifest is authoritative for:

- downstream discovery (machine-readable)
- ownership resolution for cross-tenant reuse

---

## Caching

Modules can use caching for:

- dependencies/resources (voices, models, binaries, libraries)
- computed outputs (skip expensive work)

Caching must be deterministic and governed by:

- module cache settings (`module.yml`)
- tenant work order reuse mode (`reuse_output_type=cache`)
- cache retention policy (default in module, overridable by tenant)

### Cache retention and physical deletion
Cache retention is enforced physically by the nightly cache-prune workflow using GitHub’s cache API. Expired caches are deleted and removed from `cache_index.csv`.

### Orphan cache governance (required)
Cache-prune begins by listing **all** caches and registering any cache not present in `cache_index.csv` as an **orphan** with a default **1-year** hold period. These are appended to `cache_index.csv` so administrators can review and adjust.

---

## Billing

Billing is integer-credit based and fully auditable.

### Where billing lives
Billing state tables are stored as CSV assets in the fixed Release tag:

- `billing-state`

### Spend and Refund model
- If credits are sufficient, a **SPEND** transaction is created immediately and credits are reduced immediately.
- A **REFUND** transaction is always created at the end (even if total is `0`), including a human-readable calculation note.

### Promos and deals
Promos/deals are modeled as separate **negative** line items. Promo redemptions are tracked as events:

- `APPLIED` and `REFUNDED` only.

Promo refund allocation is deterministic:

- apply order within a work order.

---

## Reason Codes and Policy

Reason codes are numeric and stable: **9 digits** `GCCMMMRRR`

- `G`: `0` Global, `1` Module
- `CC`: category id `01–99`
- `MMM`: module id `001–999` (`000` for global)
- `RRR`: reason id `001–999`

Reason definitions come from:

- `config/global_reasons.yml` (global)
- `modules/<module_id>/validation.yml` (module)

Maintenance assigns numeric codes and writes:

- `maintenance-state/reason_catalog.csv`

Admins control behavior via:

- `maintenance-state/reason_policy.csv`

Policy booleans:

- `fail=true|false` (FAILED vs COMPLETED-with-exception)
- `refundable=true|false` (applies only if FAILED)

---

## Tenant Isolation and Cross-Tenant Reuse

### Default isolation
Tenants may consume only their own releases.

### Sharing policy
A tenant can allow other tenants to consume its releases via:

- `tenants/<target_tenant>/tenant.yml: allow_release_consumers: [...]`

Maintenance compiles:

- `maintenance-state/tenant_relationships.csv` as directed pairs:
  - `(source_tenant_id, target_tenant_id)` pair presence means allowed

### Ownership is derived from manifest
When reuse mode is `release`, the orchestrator:

1) downloads the release manifest
2) reads `owning_tenant_id`
3) checks `(requesting_tenant, owning_tenant)` in `tenant_relationships.csv`

---

## Workflows

### `maintenance.yml`
Compiles and commits repo-native maintenance-state CSVs:

- reason catalog + ensures policy scaffolding
- tenant relationships
- dependencies index
- requirements index
- allocator registries
- platform artifact policy scaffolding (optional)

### `orchestrator.yml`
Runs enabled work orders with billing:

- downloads billing CSV assets from `billing-state`
- credit check → SPEND immediately
- executes modules (via `run-module.yml`)
- computes refund eligibility based on `reason_policy.csv`
- writes REFUND (always)
- updates promo redemption events
- uploads updated billing assets back to `billing-state`

### `run-module.yml`
Generic runner:

- resolves reuse mode (new/cache/release/assets)
- invokes module composite action
- handles caching, output manifest generation, artifact publishing (if purchased and eligible)
- emits status + reason_code for logs and billing linkage

### `cache-prune.yml`
Daily at 00:00:

- downloads `cache_index.csv`
- lists all caches and registers orphans into `cache_index.csv` with 1-year hold
- deletes expired caches
- removes deleted rows from `cache_index.csv`
- uploads updated `cache_index.csv` back to `billing-state`

---

## Administrator Operations

Admins manage:

- `maintenance-state/reason_policy.csv` (fail/refundable)
- `maintenance-state/ids/*` registries (categories/modules/reason allocation state)
- `maintenance-state/module_artifacts_policy.csv` (disable artifacts per module; default allow)
- billing tables in `billing-state` (credits, pricing, promos)
- orphan cache entries in `cache_index.csv` (by editing expiry policy if you choose to support that operationally later)

---

## Docs

- `docs/architecture.md` — deep technical architecture, data flows, and invariants.
- `docs/schemas.md` — example YAML/CSV/JSON schemas and sample rows.
