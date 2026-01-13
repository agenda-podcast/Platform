# Release checklist (pre-merge)

This checklist is intended for local verification before merging into `main`.

## Required commands

Run from the repository root:

```bash
pytest -q
python scripts/ci_verify.py --phase pre
python scripts/ci_verify.py --phase post
```

## Optional offline E2E sanity (dev stubs)

These steps avoid network dependencies and rely on dev stubs when credentials are not configured.

```bash
python -m platform.cli maintenance
bash scripts/run_orchestrator.sh
python scripts/e2e_assert_chaining.py
python scripts/e2e_assert_artifacts_packaging.py
python scripts/e2e_assert_email_threshold.py
python scripts/e2e_assert_idempotency.py
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


Reference: `.github/workflows/e2e-verify.yml` runs the offline parity sequence and includes the publisher guardrail in `--no-publish` mode.
