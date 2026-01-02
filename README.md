# Platform (Modular Orchestration + Billing-State)

This repository is a reference implementation of:

- **Tenant-scoped work orders** (YAML) that request **modules** to run
- An **orchestrator** that plans dependencies, checks credits, runs modules, and writes ledgers
- A GitHub Releases-friendly **billing-state** directory that is the accounting system of record
- A repo-managed **maintenance-state** directory that provides controlled catalogs (reason codes, dependencies, policies)

## Repository layout

```text
platform/               Core library + CLI
  billing/              Billing workflows (pricing, promotions, payments, top-ups)
  orchestration/        Workorder discovery, planning, execution, billing mutations
  common/               Shared utilities (incl. ID canonicalization)

modules/                Module definitions (each module in its own folder)
tenants/                Tenant folders + workorders

maintenance-state/      Repo-managed catalogs (reason codes, policies, dependency index, etc.)
billing-state-seed/     Seed accounting state used to bootstrap a billing-state directory

scripts/                CI helpers (verification) and repo maintenance utilities
config/                 Repository configuration
.github/workflows/      CI workflows
```

## Quick start

1) Bootstrap a local billing-state directory:

```bash
mkdir -p .billing-state
cp billing-state-seed/* .billing-state/
```

2) Run orchestration:

```bash
python -m platform.cli orchestrate --runtime-dir runtime --billing-state-dir .billing-state
```

3) (Optional) Publish purchased artifacts to GitHub Releases:

```bash
python -m platform.cli orchestrate --runtime-dir runtime --billing-state-dir .billing-state --enable-github-releases
```

## ID matching policy (critical)

This repo uses fixed-width numeric IDs (e.g., `tenant_id=0000000001`, `module_id=000003`).
Some tools (notably Excel) may coerce these into numbers and drop leading zeros.

To prevent ledger corruption and failed joins, the platform implements a strict policy:

- **Matching** uses a numeric *join key*: digits-only values are compared by numeric value
  (i.e., leading zeros are ignored).
- **Storage** writes canonical fixed-width IDs whenever the platform mutates accounting state.

Implementation lives in `platform/common/id_codec.py` and is applied across:
- orchestration credit checks and ledger writes
- payment reconciliation / manual top-ups
- dependency planning and reason-code lookups

## CI / verification

GitHub Actions runs end-to-end verification via `scripts/ci_verify.py`.
If you change schemas, outputs, or workflow behavior, extend verification accordingly.

---

License: see `LICENSE`.
