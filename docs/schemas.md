# Platform Schemas

This document defines the canonical schema fields that are required for deterministic validation and orchestration.

## One source of truth: `kind`

The platform uses an explicit `kind` classification to eliminate inference-by-naming and to ensure deterministic validation.

Authoritative definition:
- `platform/infra/models.py` defines:
  - `ModuleKind`
  - `MODULE_KIND_VALUES`

Allowed values:
- `transform`
- `packaging`
- `delivery`
- `other`

No other values are permitted.

## Module contract schema (`modules/<module_id>/module.yml`)

Required fields:
- `module_id` (string)
- `version` (string)
- `kind` (ModuleKind)

Compatibility policy:
- `kind` is required. A module contract missing `kind` is invalid and must fail validation.

## Workorder schema (`tenants/<tenant_id>/workorders/<work_order_id>.yml`)

Top-level fields:
- `work_order_id` (string, required)
- `enabled` (boolean, default: true)
- `artifacts_requested` (boolean, default: false)

Steps:
- `steps` (list, required and non-empty for enabled workorders)
- For each enabled step:
  - `step_id` (string, required)
  - `module_id` (string, required)
  - `kind` (ModuleKind, required)

Compatibility policy:
- Enabled workorders are strictly validated. Missing `kind` on any enabled step is a blocking error.
- Disabled workorders are treated as drafts. Missing `kind` is reported as a draft warning and does not fail `consistency-validate`.

Determinism rules:
- For enabled workorders, `step.kind` must match the referenced module contract `module.yml.kind`.
- Validators and orchestrator logic must use `kind` and must not infer packaging/delivery from module_id naming.
