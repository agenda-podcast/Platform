# Architecture (Deep Technical Specification)

This document is the authoritative technical architecture for the platform: state layers, workflow responsibilities, file/table schemas, deterministic rules, and invariants required for a correct and auditable run.

---

## 1) System Overview

The platform is a multi-tenant orchestration system implemented on GitHub Actions, with:

- **Repo-committed compiled configuration** (`maintenance-state/`) for fast runtime decisions
- **Release-backed financial truth** (`billing-state` tag) as CSV tables updated deterministically
- **Module execution** via composite actions under `modules/<module_id>/`
- **Work order-driven execution** under `tenants/<tenant_id>/workorders/`

Primary constraints:

- All identifiers are stable and deterministic.
- Billing is auditable and integer-based.
- Runtime is fast by avoiding expensive scans (compiled indexes are used).
- Tenant isolation is strict and enforced by compiled relationships + manifest ownership.
- Cache governance is auditable: orphans are registered with a default 1-year hold.

---

## 2) Identifiers and Naming

### 2.1 Module ID (canonical)
- Module ID is the folder name under `modules/`.
- **Required format:** three-digit numeric string `001–999`.
- All references use `module_id` (no “module name” keys anywhere).

### 2.2 Tenant ID
- Tenant folder name under `tenants/`.
- Must be stable; recommended lowercase with hyphens (e.g., `tenant-001`).

### 2.3 Work Order ID
- Filename stem under `tenants/<tenant>/workorders/`.
- Must be stable; recommended lowercase/hyphen.

### 2.4 Reason Code
Numeric 9-digit string `GCCMMMRRR`:

- `G`: 0 global / 1 module
- `CC`: category id (01–99)
- `MMM`: module id (001–999) or 000 for global
- `RRR`: reason id (001–999) allocated per scope

---

## 3) State Layers

### 3.1 Maintenance State (repo committed)
Folder: `maintenance-state/`

Purpose:
- eliminate expensive scans at runtime
- provide compiled registries and policy controls
- provide deterministic indexes (dependencies, requirements, relationships, reasons)
- provide platform artifact allow/deny table

Produced by: `maintenance.yml` and committed to default branch.

### 3.2 Billing State (Release tag)
Release tag: `billing-state`

Purpose:
- financial truth and minimal runtime logs
- pricing, credits, ledger, promo events, cache index

Updated by:
- `orchestrator.yml` (core billing tables + logs)
- `cache-prune.yml` (cache_index.csv)

---

## 4) Repository Structure (authoritative)

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
  tenant_relationships.csv
  module_dependency_index.csv
  module_requirements_index.csv
  module_artifacts_policy.csv
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
```

---

## 5) Maintenance State: Files and Schemas

### 5.1 ID registries (`maintenance-state/ids/`)

#### `category_registry.csv`
- `category_id` (2 digits)
- `category_name`
- `category_description`
- `active` (bool)

#### `module_registry.csv`
- `module_id` (3 digits; equals module folder name)
- `category_id` (2 digits; default reason category for module)
- `display_name` (optional)
- `module_description` (optional)
- `active` (bool)

#### `reason_registry.csv` (allocator state)
- `g` (0|1)
- `category_id`
- `module_id` (000 for global)
- `reason_id` (3 digits)
- `reason_key` (string, stable)
- `active` (bool)
- `notes` (optional)

Uniqueness constraints:
- unique `(g, category_id, module_id, reason_id)`
- unique `(g, category_id, module_id, reason_key)`

Allocation rule:
- allocate the **lowest unused** `reason_id` within `(g, category_id, module_id)`.

### 5.2 Reason catalog and policy

#### `reason_catalog.csv` (compiled)
- `reason_code` (GCCMMMRRR)
- `g`
- `category_id`
- `module_id`
- `reason_key`
- `category_name`
- `description`
- `scope` (GLOBAL|MODULE)

#### `reason_policy.csv` (admin-controlled)
- `reason_code`
- `fail` (bool)
- `refundable` (bool)
- `notes` (optional)

Semantics:
- `fail=true` => runtime sets `status=FAILED`
- `fail=false` => runtime sets `status=COMPLETED` but records `reason_code`
- refunds apply only to `FAILED` and require `refundable=true`

### 5.3 Tenant relationships

#### `tenant_relationships.csv`
- `source_tenant_id` (consumer)
- `target_tenant_id` (release owner)

Pair presence = allowed.

Source of truth:
- `tenants/<target>/tenant.yml: allow_release_consumers: [source...]`
Maintenance compiles directed pairs and includes self-pairs.

### 5.4 Dependencies and requirements

#### `module_dependency_index.csv`
- `module_id`
- `depends_on_module_ids` (encoded list)

#### `module_requirements_index.csv`
- `module_id`
- `requirement_type`
- `requirement_key`
- `version_or_hash`
- `source_uri`
- `cache_group`

### 5.5 Platform artifacts policy

#### `module_artifacts_policy.csv`
- `module_id`
- `platform_artifacts_enabled` (bool)

Default:
- if a row is absent, treat as `true` (enabled).
Admin may set `false`; automation must never flip admin choices.

---

## 6) Billing Configuration (Repository-managed)

Billing *configuration* is admin-managed and committed to the repo under:

`platform/billing/`

These files are used for *spend estimation* and promo eligibility.

### 6.1 Base prices

#### `platform/billing/module_prices.csv`
- `module_id` (3-digit string)
- `price_run_credits` (int)
- `price_save_to_release_credits` (int)
- `effective_from` (optional)
- `effective_to` (optional)
- `active` (bool)
- `notes` (optional)

### 6.2 Promotions

#### `platform/billing/promotions.csv`
- `promo_id`
- `code`
- `type`
- `value_credits` (int magnitude)
- `max_uses_per_tenant` (optional int)
- `valid_from` (optional)
- `valid_to` (optional)
- `active` (bool)
- `rules_json` (optional)
- `notes` (optional)

### 6.3 Top-up instructions

#### `platform/billing/topup_instructions.csv`
Operational instructions for how admins (and later automation) perform top-ups.

---

## 7) Billing State (Release-managed, source of truth)

Billing *state* is the accounting source of truth and is stored as Release assets (e.g., `billing-state-v1`).
These files must not be edited manually; they are updated by GitHub Actions.

A manifest (`state_manifest.json`) should be uploaded last to indicate a complete update.

### 7.1 Tenant credits
#### `tenants_credits.csv`
- `tenant_id`
- `credits_available` (int)
- `updated_at`
- `status` (active|suspended)

### 7.2 Ledger

#### `transactions.csv`
- `transaction_id`
- `tenant_id`
- `work_order_id` (nullable)
- `type` (SPEND|REFUND|TOPUP|ADJUSTMENT)
- `total_amount_credits` (int; may be 0)
- `created_at`
- `metadata_json` (refund calc summary, etc.)

#### `transaction_items.csv`
- `transaction_item_id`
- `transaction_id`
- `tenant_id`
- `work_order_id`
- `module_run_id` (nullable)
- `name` (`module:<id>`, `upload:<id>`, `promo:<code>`, `deal:<id>`, `refund_note`)
- `category` (MODULE_RUN|UPLOAD|PROMO|DEAL|REFUND_NOTE|OTHER)
- `amount_credits` (int; 0 allowed)
- `reason_code` (nullable)
- `note` (optional)

Rules:
- base module items are positive
- promos/deals are negative line items
- refunds are REFUND transactions; itemization is allowed

### 7.3 Minimal operational logs (Option B)

#### `workorders_log.csv`
- `work_order_id`
- `tenant_id`
- `status` (IN_PROGRESS|COMPLETED|PARTIALLY_COMPLETED|FAILED)
- `reason_code` (nullable)
- `started_at`
- `finished_at`
- `github_run_id`
- `workorder_mode` (STRICT|PARTIAL_ALLOWED)
- `requested_modules` (encoded list)
- `metadata_json` (optional)

#### `module_runs_log.csv`
- `module_run_id`
- `work_order_id`
- `tenant_id`
- `module_id`
- `status` (COMPLETED|FAILED)
- `reason_code` (nullable)
- `started_at`
- `finished_at`
- `reuse_output_type` (new|cache|release|assets)
- `reuse_reference` (release tag or assets folder; nullable)
- `cache_key_used` (nullable)
- `published_release_tag` (nullable)
- `release_manifest_name` (nullable)
- `metadata_json` (optional)

### 7.4 Promotion redemption events

Promo definitions are repo-managed (`platform/billing/promotions.csv`). Usage events and auditability remain release-managed.

#### `promotion_redemptions.csv`
- `event_id`
- `tenant_id`
- `promo_id`
- `work_order_id`
- `event_type` (APPLIED|REFUNDED)
- `amount_credits` (int; discount amount is stored as a negative line item, but redemptions keep the signed amount)
- `created_at`
- `note` (optional)

### 7.5 Cache governance

#### `cache_index.csv`
- `cache_key`
- `tenant_id` (best effort)
- `module_id` (best effort)
- `created_at`
- `expires_at`
- `cache_id` (recommended)

**Row deletion rule:** when a cache is physically deleted, its row is removed from `cache_index.csv`.

**Orphan registration rule (required):**
- At the start of cache-prune, list all caches via API.
- If a cache is not listed in `cache_index.csv`, append it with `expires_at = created_at + 1 year` and persist the updated index.

---

## 7) Work Order Execution Semantics (Orchestrator)

### 7.1 Discovery
Orchestrator discovers enabled work orders under `tenants/*/workorders/*.yml`.

### 7.2 Credit check and Spend timing (no preflight)
- If credits insufficient: Work Order fails with reason `not_enough_credits`; no Spend created.
- If sufficient: create SPEND immediately; reduce credits immediately.

### 7.3 Dependency planning
Order modules using `module_dependency_index.csv` (topological sort).

### 7.4 Reuse modes
Per module:
- `new`: execute module
- `cache`: if cache hit exists => record module run as `FAILED` with reason `skipped_cache` (numeric code); module is refunded if refundable
- `release`: download release manifest + items by tag; enforce sharing via `tenant_relationships.csv`
- `assets`: read from `tenants/<tenant>/assets/outputs/<folder>/manifest.json`

### 7.5 Artifact purchase/eligibility
Tenant may purchase artifacts per module. Eligibility requires:
1) module supports artifacts
2) platform artifacts enabled (default allow unless disabled)
3) tenant purchase flag true

If tenant purchases but artifacts are not eligible: module must fail.

---

## 8) Refund Semantics

### 8.1 Refund transaction always
At end of work order:
- create REFUND transaction always (0 allowed)
- include a calculation note item

### 8.2 Refund eligibility uses reason_policy
For each FAILED module:
- refund only if `reason_policy.refundable=true` for its reason_code

### 8.3 Refund amount rule (net of deals/promos)
- `failed_gross` = sum of charged items for refundable failed modules (base + uploads)
- `deals_total` = abs(sum of promo/deal negative items)
- `refundable_net` = max(0, failed_gross - deals_total)

Refund transaction total = `-refundable_net`.

---

## 9) Releases: Artifact Naming and Manifest

### 9.1 Work order release tag
- `wo-<tenant_id>-<work_order_id>`

### 9.2 File naming
- `tenantid-work_orderid-moduleid-itemid-shorthash.extension`

### 9.3 One manifest per release
- `tenantid-work_orderid-manifest.json`
Includes:
- `owning_tenant_id`
- `work_order_id`
- items with filename, module_id, hashes, sizes

Ownership is derived from the manifest.

---

## 10) Cache Prune (single workflow with required inventory + orphan registration)

Cache prune is the only place where caches are physically deleted.

1) Download `cache_index.csv`
2) List all caches via API
3) Register orphans into `cache_index.csv` with `expires_at = created_at + 1 year`
4) Delete expired indexed caches
5) Remove rows for deleted caches
6) Upload updated `cache_index.csv` back to `billing-state`

---

## 11) Invariants

1) Module ID equals module folder name and is the only key used everywhere.
2) Reason codes are numeric and stable; `reason_policy.csv` is the sole source of fail/refund behavior.
3) Spend happens immediately after credit check; no preflight gating.
4) Refund transaction is always created (including zero).
5) Cross-tenant release reuse is allowed only if:
   - owning tenant is read from manifest, and
   - relationship pair exists in `tenant_relationships.csv`
6) Orphan caches are always tracked:
   - registered into `cache_index.csv` with a default 1-year hold.
7) Billing-state assets are overwritten deterministically; optional `state_manifest.json` is written last.
