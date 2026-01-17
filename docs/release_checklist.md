# Release checklist (pre-merge)

This checklist is intended for local verification before merging into `main`.

## Required commands

Run from the repository root:

```bash
pytest -q
python scripts/ci_verify.py --phase pre
python scripts/ci_verify.py --phase post
```

## Optional offline publish guardrail (no-publish)

This step is the preferred offline smoke-check for the publisher script. It validates that the script imports, loads the registries, and executes without requiring a prebuilt runtime snapshot.

```bash
set -euo pipefail
rm -rf dist_artifacts_guardrail runtime-guardrail
mkdir -p runtime-guardrail
python scripts/publish_artifacts_release.py \
  --runtime-profile config/runtime_profile.dev_github.yml \
  --billing-state-dir billing-state-seed \
  --runtime-dir runtime-guardrail \
  --dist-dir dist_artifacts_guardrail \
  --since 2100-01-01T00:00:00Z \
  --no-publish
```

## Policy checkpoints (must hold)

### Activation gating

- If `artifacts_requested: true` then at least one enabled `packaging` step and at least one enabled `delivery` step must exist.
- If any enabled `packaging` step exists (regardless of `artifacts_requested`) then an enabled `delivery` step must exist.
- No auto-injection of steps. Validation blocks activation only when `enabled: true`. Drafts (`enabled: false`) produce warnings only.

### Email attachment cap

- `deliver_email` hard-fails when `package_zip.bytes >= 19.9 MiB` using `reason_slug=package_too_large_for_email`.
- No link fallback is allowed.

### Refund safety

- Refunds are permitted only when verification confirms non-delivery or the failure is deterministic non-delivery.
- Delivery receipts must include `provider`, `remote_path` or object id, `bytes`, and `verification_status`.


Reference: `.github/workflows/maintenance.yml` (Publish script guardrail step) runs the offline parity sequence and includes the publisher guardrail in `--no-publish` mode.

## Verification workflows (recommended)

In addition to `ci_verify.py`, the repository provides explicit verification workflows:

- **Verify Platform**: `python scripts/verify_platform.py`
- **Verify Modules**: `python scripts/verify_module.py --module-id <module_id>`
- **Verify Workorders**: `python scripts/verify_workorder.py --work-order-id <work_order_id>`

See `docs/verification.md` for responsibilities, dropdown generation, and manual override behavior.
