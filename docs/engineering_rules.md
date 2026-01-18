# Engineering rules (must hold across the repo)

This document defines non-negotiable engineering invariants for PLATFORM. Code changes must preserve these invariants.

## Source of truth: Billing

- **Billing is the source of truth** for all billable actions and outcomes.
- The canonical ledger is the pair:
  - `.billing-state/transactions.csv`
  - `.billing-state/transaction_items.csv`
- Other files in `.billing-state/` are supporting indexes (for example `cache_index.csv`, `github_*_map.csv`).
- Do not create parallel “shadow ledgers” elsewhere.

### Run → spend → refund visibility

- Every orchestrator attempt that reaches a billable boundary must be visible in the billing ledger.
- If an action fails and the reason is refundable, the ledger must show:
  - a SPEND (negative) line item(s)
  - a REFUND (positive) line item(s)
  - matching idempotency keys to prevent duplicates on rerun
- Insufficient credits must result in a deterministic failure reason (for example `not_enough_credits`) and still be audit-visible.

## Secrets: Secretstore only

- Secrets are never hardcoded.
- Modules declare required secrets in their `module.yml` and read them via the **secretstore loader**.
- Workorders provide inputs only; they do not embed secrets.
- CI/workflows pass secrets by environment variables and/or secretstore material only.

## Strict role separation

- **Modules**: functional work only, constrained to their own folder + declared inputs; outputs only via ports.
- **Workorders**: orchestration intent and inputs (what to run, with what inputs).
- **Orchestrator**: execution engine (step resolution, sequencing, runtime outputs, billing posts, status reduction).
- **Maintenance**: registries and deterministic indexes (module IDs, workorder IDs, dropdown lists).
- **Billing-state publisher**: release asset publish only (workflow responsibility).

No blended responsibilities and no duplicated work across layers.

## Determinism and idempotency

- Maintenance outputs are deterministic (stable ordering).
- Orchestrator posts billing using **idempotency keys** so reruns do not duplicate spends/refunds.
- Cache policy is governed by `platform/config/platform_config.yml` and written to `cache_index.csv` with schema:
  - `place,type,ref,created_at,expires_at`

## Code structure constraints

- All non-CSV code/logic files must be **≤ 500 lines**.
- Splits must be **functional and role-based**:
  - file and folder names must describe responsibility
  - no mechanical split naming (for example `chunk_01.py`)
- CSV tables are exempt from the 500-line limit.

## Operational invariants

- All actions that can affect outcomes must be billed (or explicitly and deterministically non-billable).
- After a failure, refundable actions must be refunded with clear reason codes.
- Reason codes and refundability are governed by the billing reason catalog (not hardcoded logic).
