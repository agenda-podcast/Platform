from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .maintenance.builder import run_maintenance
from .orchestration.orchestrator import run_orchestrator
from .cache.prune import run_cache_manage, run_cache_prune
from .orchestration.module_exec import execute_module_runner

from .billing.state import BillingState
from .billing.payments import (
    reconcile_repo_payments_into_billing_state,
    validate_repo_payments,
)
from .billing.admin_topup import append_admin_topup_payment

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

    run_orchestrator(
        repo_root=repo_root,
        billing_state_dir=billing_state_dir,
        runtime_dir=runtime_dir,
        enable_github_releases=enable_releases,
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
    out = execute_module_runner(module_path, params, outputs_dir)

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

def cmd_admin_topup(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    tenant_id = str(args.tenant_id or "").strip()

    try:
        amount = int(str(args.amount_credits).strip())
    except Exception as e:
        raise SystemExit("amount_credits must be an integer") from e

    res = append_admin_topup_payment(
        repo_root=repo_root,
        tenant_id=tenant_id,
        amount_credits=amount,
        reference=str(getattr(args, "reference", "") or "").strip(),
        note=str(getattr(args, "note", "") or "").strip(),
        status="CONFIRMED",
    )

    print(
        json.dumps(
            {
                "payment_id": res.payment_id,
                "tenant_id": res.tenant_id,
                "topup_method_id": res.topup_method_id,
                "amount_credits": res.amount_credits,
                "received_at": res.received_at,
                "reference": res.reference,
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

def cmd_cache_prune(args: argparse.Namespace) -> int:
    # Backwards-compatible command name.
    # Note: cache management is repo-scoped and uses the repo-managed cache index file
    # under platform/cache/cache_index.csv.
    res = run_cache_manage(
        repo_root=_repo_root(),
        cache_index_path=Path(getattr(args, "cache_index_path", "") or (_repo_root() / "platform" / "cache" / "cache_index.csv")),
        apply=bool(getattr(args, "apply", False)) and not bool(getattr(args, "dry_run", False)),
        delete_key=str(getattr(args, "delete_key", "") or "").strip(),
        delete_prefix=str(getattr(args, "delete_prefix", "") or "").strip(),
    )
    print(
        json.dumps(
            {
                "rules": res.rules,
                "caches_seen": res.caches_seen,
                "caches_indexed": res.caches_indexed,
                "deleted_caches": res.deleted_caches,
                "would_delete_caches": res.would_delete_caches,
                "skipped_protected": res.skipped_protected,
            }
        )
    )
    return 0


def cmd_cache_manage(args: argparse.Namespace) -> int:
    res = run_cache_manage(
        repo_root=_repo_root(),
        cache_index_path=Path(args.cache_index_path).resolve(),
        apply=bool(args.apply),
        delete_key=str(args.delete_key or "").strip(),
        delete_prefix=str(args.delete_prefix or "").strip(),
    )
    print(
        json.dumps(
            {
                "rules": res.rules,
                "caches_seen": res.caches_seen,
                "caches_indexed": res.caches_indexed,
                "deleted_caches": res.deleted_caches,
                "would_delete_caches": res.would_delete_caches,
                "skipped_protected": res.skipped_protected,
            }
        )
    )
    return 0

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="platform")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("maintenance", help="Compile maintenance-state tables")
    sp.set_defaults(func=cmd_maintenance)

    sp = sub.add_parser("orchestrate", help="Run orchestrator")
    sp.add_argument("--runtime-dir", default="runtime")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--enable-github-releases", action="store_true")
    sp.set_defaults(func=cmd_orchestrate)

    sp = sub.add_parser("orchestrator", help="Alias for orchestrate")
    sp.add_argument("--runtime-dir", default="runtime")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.add_argument("--enable-github-releases", action="store_true")
    sp.set_defaults(func=cmd_orchestrate)

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

    sp = sub.add_parser("cache-manage", help="Centralized GitHub Actions cache management")
    sp.add_argument("--cache-index-path", default=str(_repo_root() / "platform" / "cache" / "cache_index.csv"))
    sp.add_argument("--apply", action="store_true", help="Actually delete caches (otherwise dry-run)")
    sp.add_argument("--delete-key", default="", help="Surgically delete a specific cache key")
    sp.add_argument("--delete-prefix", default="", help="Surgically delete all cache keys with this prefix")
    sp.set_defaults(func=cmd_cache_manage)

    # Backwards compatibility: old command name
    sp = sub.add_parser("cache-prune", help="Alias for cache-manage")
    sp.add_argument("--cache-index-path", default=str(_repo_root() / "platform" / "cache" / "cache_index.csv"))
    sp.add_argument("--apply", action="store_true")
    sp.add_argument("--dry-run", action="store_true")
    sp.add_argument("--delete-key", default="")
    sp.add_argument("--delete-prefix", default="")
    sp.set_defaults(func=cmd_cache_prune)

    sp = sub.add_parser("validate-payments", help="Validate repo payments.csv before reconciliation")
    sp.set_defaults(func=cmd_validate_payments)

    sp = sub.add_parser("reconcile-payments", help="Reconcile repo-recorded payments into billing-state")
    sp.add_argument("--billing-state-dir", default=".billing-state")
    sp.set_defaults(func=cmd_reconcile_payments)

    sp = sub.add_parser("admin-topup", help="Append an Admin Top Up record into platform/billing/payments.csv")
    sp.add_argument("--tenant-id", required=True)
    sp.add_argument("--amount-credits", required=True, dest="amount_credits")
    sp.add_argument("--reference", default="")
    sp.add_argument("--note", default="")
    sp.set_defaults(func=cmd_admin_topup)

    return p

def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args) or 0)

if __name__ == "__main__":
    raise SystemExit(main())
