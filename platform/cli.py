from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .infra.config import load_runtime_profile
from .infra.factory import build_infra

from .maintenance.builder import run_maintenance
from .orchestration.orchestrator import run_orchestrator
from .cache.prune import run_cache_prune
from .orchestration.module_exec import execute_module_runner

from .secretstore.loader import load_secretstore, env_for_module

from .billing.state import BillingState
from .billing.topup import TopupRequest, apply_admin_topup
from .billing.payments import (
    reconcile_repo_payments_into_billing_state,
    validate_repo_payments,
)

from .consistency.validator import validate_all_workorders, integrity_validate


def _repo_root() -> Path:
    # This file is at: <repo_root>/platform/cli.py
    return Path(__file__).resolve().parents[1]


def cmd_maintenance(args: argparse.Namespace) -> int:
    run_maintenance(repo_root=_repo_root())
    return 0


def cmd_orchestrate(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    runtime_dir = Path(args.runtime_dir).resolve()
    billing_state_dir = Path(args.billing_state_dir).resolve()
    enable_releases = bool(args.enable_github_releases)

    runtime_dir.mkdir(parents=True, exist_ok=True)
    billing_state_dir.mkdir(parents=True, exist_ok=True)

    profile = load_runtime_profile(repo_root, cli_path=str(getattr(args, "runtime_profile", "") or ""))
    infra = build_infra(
        repo_root=repo_root,
        profile=profile,
        billing_state_dir=billing_state_dir,
        runtime_dir=runtime_dir,
    )

    run_orchestrator(
        repo_root=repo_root,
        billing_state_dir=billing_state_dir,
        runtime_dir=runtime_dir,
        enable_github_releases=enable_releases,
        infra=infra,
    )
    return 0


def cmd_module_exec(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    module_id = args.module_id

    # Allow module_path override for GitHub composite actions; defaults to repo/modules/<id>.
    module_path = Path(getattr(args, "module_path", "") or "")
    if not str(module_path):
        module_path = repo_root / "modules" / module_id

    params = json.loads(args.params_json)
    outputs_dir = Path(args.outputs_dir).resolve()
    store = load_secretstore(repo_root)
    module_env = env_for_module(store, module_id)
    out = execute_module_runner(module_path, params, outputs_dir, env=module_env)

    # Composite action compatibility: optionally set GitHub step outputs.
    if bool(getattr(args, "github_action_outputs", False)):
        out_path = os.environ.get("GITHUB_OUTPUT")
        if out_path:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write("status=COMPLETED\n")
                f.write("reason_code=\n")
                f.write("cache_key=\n")
                f.write("manifest_item_json=\n")

    print(json.dumps(out))
    return 0


def cmd_validate_payments(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    report = validate_repo_payments(repo_root)

    # Human-readable summary (helpful in Actions logs)
    print(
        "payments.csv validation OK: "
        f"payments_seen={report.payments_seen}, eligible_seen={report.eligible_seen}, warnings={len(report.warnings)}"
    )
    for w in report.warnings:
        print(f"WARNING: {w}")

    # Structured output for automation
    print(
        json.dumps(
            {
                "payments_seen": report.payments_seen,
                "eligible_seen": report.eligible_seen,
                "warnings": report.warnings,
            }
        )
    )
    return 0


def cmd_reconcile_payments(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    billing = BillingState(Path(args.billing_state_dir))

    billing.validate_minimal(
        required_files=[
            "tenants_credits.csv",
            "transactions.csv",
            "transaction_items.csv",
            "promotion_redemptions.csv",
            "cache_index.csv",
            "workorders_log.csv",
            "module_runs_log.csv",
            "github_releases_map.csv",
            "github_assets_map.csv",
        ]
    )

    res = reconcile_repo_payments_into_billing_state(repo_root, billing)
    billing.write_state_manifest()

    if res.payments_applied:
        # Marker for workflows that conditionally upload updated Release assets.
        marker = Path(args.billing_state_dir) / ".billing_changed"
        marker.write_text(str(res.payments_applied), encoding="utf-8")

    # Human-readable summary (helpful in Actions logs)
    print(
        "payments reconciliation complete: "
        f"payments_seen={res.payments_seen}, eligible={res.payments_eligible}, "
        f"applied={res.payments_applied}, skipped_already_applied={res.payments_skipped_already_applied}"
    )
    if res.applied_transaction_ids:
        print("applied_transaction_ids:")
        for tx in res.applied_transaction_ids:
            print(f"- {tx}")

    # Structured output for automation
    print(
        json.dumps(
            {
                "payments_seen": res.payments_seen,
                "payments_eligible": res.payments_eligible,
                "payments_applied": res.payments_applied,
                "payments_skipped_already_applied": res.payments_skipped_already_applied,
                "applied_transaction_ids": res.applied_transaction_ids,
            }
        )
    )
    return 0


def cmd_admin_topup(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    billing_state_dir = Path(args.billing_state_dir).resolve()
    billing_state_dir.mkdir(parents=True, exist_ok=True)
    billing = BillingState(billing_state_dir)
    billing.validate_minimal(
        required_files=[
            "tenants_credits.csv",
            "transactions.csv",
            "transaction_items.csv",
            "promotion_redemptions.csv",
            "cache_index.csv",
            "workorders_log.csv",
            "module_runs_log.csv",
            "github_releases_map.csv",
            "github_assets_map.csv",
        ]
    )

    req = TopupRequest(
        tenant_id=str(args.tenant_id),
        amount_credits=int(args.amount_credits),
        topup_method_id=str(args.topup_method_id),
        reference=str(args.reference),
        note=str(args.note or ""),
    )
    tx_id = apply_admin_topup(repo_root, billing, req)
    billing.write_state_manifest()
    print(json.dumps({"transaction_id": tx_id, "tenant_id": req.tenant_id, "amount_credits": req.amount_credits}))
    return 0


def cmd_cache_prune(args: argparse.Namespace) -> int:
    res = run_cache_prune(Path(args.billing_state_dir).resolve(), dry_run=bool(args.dry_run))
    print(
        json.dumps(
            {
                "updated_rows": res.updated_rows,
                "deleted_caches": res.deleted_caches,
                "registered_orphans": res.registered_orphans,
            }
        )
    )
    return 0


def cmd_consistency_validate(args: argparse.Namespace) -> int:
    """Validate workorders against module contracts (servicing tables).

    Behavior:
    - Enabled workorders: blocking validation. Any failure returns non-zero.
    - Disabled workorders: draft warnings are printed, but exit code remains zero.
    """
    try:
        validate_all_workorders(_repo_root())
        return 0
    except Exception as e:
        # The validator raises ConsistencyValidationError for blocking failures.
        print(str(e))
        return 2



def cmd_integrity_validate(args: argparse.Namespace) -> int:
    # Integrity Validation (plan/preflight only, no execution).
    # Supports validating a single workorder (by id/path) or all enabled workorders in the index.
    repo_root = _repo_root()
    results = integrity_validate(
        repo_root,
        work_order_id=str(getattr(args, "work_order_id", "") or ""),
        tenant_id=str(getattr(args, "tenant_id", "") or ""),
        path=str(getattr(args, "path", "") or ""),
    )

    # Human-readable summary (helpful in Actions logs)
    print(f"integrity validation OK: workorders_validated={len(results)}")

    # Structured output for automation
    import json as _json
    print(_json.dumps({"validated": results}, ensure_ascii=False))
    return 0


def cmd_runtime_print(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    profile = load_runtime_profile(repo_root, cli_path=str(getattr(args, 'runtime_profile', '') or ''))

    billing_state_dir = Path(str(getattr(args, 'billing_state_dir', '') or '.billing-state')).resolve()
    runtime_dir = Path(str(getattr(args, 'runtime_dir', '') or 'runtime')).resolve()

    infra = build_infra(
        repo_root=repo_root,
        profile=profile,
        billing_state_dir=billing_state_dir,
        runtime_dir=runtime_dir,
    )
    print(json.dumps(infra.describe(), indent=2, sort_keys=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="platform")
    p.add_argument('--runtime-profile', default='', help='Path to runtime profile YAML (overrides PLATFORM_RUNTIME_PROFILE and config/runtime_profile.yml)')
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("maintenance", help="Compile maintenance-state tables")
    sp.set_defaults(func=cmd_maintenance)

    sp = sub.add_parser("orchestrate", help="Run orchestrator")
    sp.add_argument('--runtime-profile', default='', help=argparse.SUPPRESS)
    sp.add_argument("--runtime-dir", default="runtime")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--enable-github-releases", action="store_true")
    sp.set_defaults(func=cmd_orchestrate)

    sp = sub.add_parser("orchestrator", help="Alias for orchestrate")
    sp.add_argument('--runtime-profile', default='', help=argparse.SUPPRESS)
    sp.add_argument("--runtime-dir", default="runtime")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--enable-github-releases", action="store_true")
    sp.set_defaults(func=cmd_orchestrate)

    sp = sub.add_parser("consistency-validate", help="Consistency Validation (data-driven, pre-exec)")
    sp.set_defaults(func=cmd_consistency_validate)


    sp = sub.add_parser("integrity-validate", help="Integrity Validation (plan/preflight only, no execution)")
    sp.add_argument("--work-order-id", default="")
    sp.add_argument("--tenant-id", default="")
    sp.add_argument("--path", default="")
    sp.set_defaults(func=cmd_integrity_validate)


    sp = sub.add_parser("module-exec", help="Execute a single module runner")
    sp.add_argument("--module-id", required=True)
    sp.add_argument("--params-json", required=True)
    sp.add_argument("--outputs-dir", required=True)
    sp.add_argument("--tenant-id", default="")
    sp.add_argument("--work-order-id", default="")
    sp.add_argument("--module-run-id", default="")
    sp.add_argument("--runtime-dir", default="")
    sp.add_argument("--module-path", default="")
    sp.add_argument("--github-action-outputs", action="store_true")
    sp.set_defaults(func=cmd_module_exec)

    sp = sub.add_parser("cache-prune", help="Prune Actions caches and update cache_index")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--dry-run", action="store_true")
    sp.set_defaults(func=cmd_cache_prune)

    sp = sub.add_parser("admin-topup", help="Admin: apply a ledger top-up to billing-state")
    sp.add_argument("--tenant-id", required=True)
    sp.add_argument("--amount-credits", required=True)
    sp.add_argument("--topup-method-id", required=True)
    sp.add_argument("--reference", required=True)
    sp.add_argument("--note", default="")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.set_defaults(func=cmd_admin_topup)

    sp = sub.add_parser("validate-payments", help="Validate repo payments.csv before reconciliation")
    sp.set_defaults(func=cmd_validate_payments)

    sp = sub.add_parser("reconcile-payments", help="Reconcile repo-recorded payments into billing-state")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.set_defaults(func=cmd_reconcile_payments)

    sp = sub.add_parser("runtime-print", help="Print runtime profile adapter wiring (dry-run)")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--runtime-dir", default="runtime")
    sp.set_defaults(func=cmd_runtime_print)


    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
