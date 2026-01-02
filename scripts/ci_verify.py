#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

MODULE_ID_RE = re.compile(r"^\d{6}$")
TENANT_ID_RE = re.compile(r"^\d{10}$")


def _die(msg: str) -> None:
    print(f"[CI_VERIFY][FAIL] {msg}", file=sys.stderr)
    raise SystemExit(2)


def _ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")


def _read_csv_header(path: Path) -> List[str]:
    if not path.exists():
        _die(f"Missing CSV: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            _die(f"Empty CSV (no header): {path}")
    return [h.strip() for h in header]


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    _ = _read_csv_header(path)
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            _die(f"CSV has no header: {path}")
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
    return rows


def _assert_exact_header(path: Path, expected: List[str]) -> None:
    got = _read_csv_header(path)
    if got != expected:
        _die(
            "CSV header mismatch:\n"
            f"  file: {path}\n"
            f"  expected: {expected}\n"
            f"  got:      {got}"
        )


def _ensure_file(path: Path, header: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _verify_platform_billing(repo_root: Path) -> None:
    billing_dir = repo_root / "platform" / "billing"
    if not billing_dir.exists():
        _die("Missing repo folder: platform/billing")

    module_prices = billing_dir / "module_prices.csv"
    promotions = billing_dir / "promotions.csv"
    topup_instr = billing_dir / "topup_instructions.csv"
    payments = billing_dir / "payments.csv"

    _assert_exact_header(
        module_prices,
        [
            "module_id",
            "price_run_credits",
            "price_save_to_release_credits",
            "effective_from",
            "effective_to",
            "active",
            "notes",
        ],
    )
    _assert_exact_header(
        promotions,
        [
            "promo_id",
            "code",
            "type",
            "value_credits",
            "max_uses_per_tenant",
            "valid_from",
            "valid_to",
            "active",
            "rules_json",
            "notes",
        ],
    )
    _assert_exact_header(
        topup_instr,
        [
            "topup_method_id",
            "channel",
            "status",
            "currency",
            "min_amount",
            "fee_notes",
            "processing_time",
            "admin_action_required",
            "reference_format",
            "instructions",
        ],
    )
    _assert_exact_header(
        payments,
        [
            "payment_id",
            "tenant_id",
            "topup_method_id",
            "amount_credits",
            "reference",
            "received_at",
            "status",
            "note",
        ],
    )

    rows = _read_csv_rows(module_prices)
    for i, r in enumerate(rows, start=2):
        mid = r.get("module_id", "")
        if mid and not MODULE_ID_RE.match(mid):
            _die(f"platform/billing/module_prices.csv invalid module_id at line {i}: {mid!r}")

    _ok("Repo billing config: headers + basic validation OK")


def _verify_maintenance_state(repo_root: Path) -> None:
    ms = repo_root / "maintenance-state"
    if not ms.exists():
        _die("Missing maintenance-state/ folder")

    required = [
        ms / "reason_catalog.csv",
        ms / "reason_policy.csv",
        ms / "module_dependency_index.csv",
        ms / "module_artifacts_policy.csv",
        ms / "tenant_relationships.csv",
        ms / "ids" / "module_registry.csv",
    ]
    for p in required:
        if not p.exists():
            _die(f"Missing maintenance-state required file: {p}")

    _ok("Maintenance-state: required files present")


def _verify_dependency_index(repo_root: Path) -> None:
    dep = repo_root / "maintenance-state" / "module_dependency_index.csv"
    rows = _read_csv_rows(dep)
    seen = False
    for r in rows:
        if str(r.get("module_id", "")).strip() == "000002":
            seen = True
            depends = str(r.get("depends_on_module_ids", "")).strip()
            if "000001" not in depends:
                _die("module_dependency_index.csv: module 000002 must depend on 000001")
    if not seen:
        _die("module_dependency_index.csv: missing module 000002 row")
    _ok("Dependency index: module 000002 depends on 000001")


def _verify_billing_state_dir(billing_state_dir: Path) -> None:
    expected_headers = {
        "tenants_credits.csv": ["tenant_id", "credits_available", "updated_at", "status"],
        "transactions.csv": ["transaction_id", "tenant_id", "work_order_id", "type", "total_amount_credits", "created_at", "metadata_json"],
        "transaction_items.csv": ["transaction_item_id", "transaction_id", "tenant_id", "work_order_id", "module_run_id", "name", "category", "amount_credits", "reason_code", "note"],
        "promotion_redemptions.csv": ["event_id", "tenant_id", "promo_id", "work_order_id", "event_type", "amount_credits", "created_at", "note"],
        "cache_index.csv": ["cache_key", "tenant_id", "module_id", "created_at", "expires_at", "cache_id"],
        "workorders_log.csv": ["work_order_id", "tenant_id", "status", "reason_code", "started_at", "finished_at", "github_run_id", "workorder_mode", "requested_modules", "metadata_json"],
        "module_runs_log.csv": ["module_run_id", "work_order_id", "tenant_id", "module_id", "status", "reason_code", "started_at", "finished_at", "reuse_output_type", "reuse_reference", "cache_key_used", "published_release_tag", "release_manifest_name", "metadata_json"],
    }

    for fname, hdr in expected_headers.items():
        _ensure_file(billing_state_dir / fname, hdr)
        _assert_exact_header(billing_state_dir / fname, hdr)

    runs = _read_csv_rows(billing_state_dir / "module_runs_log.csv")
    for r in runs:
        mid = r.get("module_id", "")
        if mid and not MODULE_ID_RE.match(mid):
            _die(f"module_runs_log.csv invalid module_id: {mid!r} (expected 6 digits)")
        tid = r.get("tenant_id", "")
        if tid and not TENANT_ID_RE.match(tid):
            _die(f"module_runs_log.csv invalid tenant_id: {tid!r} (expected 10 digits)")

    _ok(f"Billing-state: required files + headers OK in {billing_state_dir}")


def _iter_files_recursive(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*") if p.is_file()]


def _mid_variants(mid6: str) -> List[str]:
    """Return variants for matching legacy folder naming (3-digit / non-padded)."""
    if not MODULE_ID_RE.match(mid6):
        return [mid6]
    n = int(mid6)
    mid3 = f"{n:03d}"
    return [mid6, mid3, str(n)]


def _find_module_output_dir(wo_dir: Path, module_id_6: str, module_run_id: str) -> Optional[Path]:
    variants = _mid_variants(module_id_6)
    direct_candidates: List[Path] = []
    for v in variants:
        direct_candidates.extend([
            wo_dir / f"module-{v}",
            wo_dir / f"module_{v}",
            wo_dir / v,
            wo_dir / f"output-{v}",
            wo_dir / f"output_{v}",
            wo_dir / "modules" / v,
            wo_dir / "modules" / f"module-{v}",
        ])
    for p in direct_candidates:
        if p.exists() and p.is_dir():
            return p

    # Prefer direct children match
    children = [p for p in wo_dir.iterdir() if p.is_dir()]
    for p in children:
        if any(v in p.name for v in variants) or (module_run_id and module_run_id in p.name):
            return p

    # Last resort: recursive within this workorder dir only
    for p in wo_dir.rglob("*"):
        if p.is_dir() and (any(v in p.name for v in variants) or (module_run_id and module_run_id in p.name)):
            return p
    return None


def _dump_wo_dirs(wo_dir: Path) -> str:
    if not wo_dir.exists():
        return "<missing>"
    names = sorted([p.name for p in wo_dir.iterdir() if p.is_dir()])
    return ", ".join(names) if names else "<no subdirs>"


def _verify_runtime_outputs(runtime_dir: Path, billing_state_dir: Path) -> None:
    wo_root = runtime_dir / "workorders"
    if not wo_root.exists():
        _die(f"Missing runtime/workorders folder: {wo_root}")

    runs_path = billing_state_dir / "module_runs_log.csv"
    runs_all = _read_csv_rows(runs_path)
    if not runs_all:
        _die(f"module_runs_log.csv has no rows in {runs_path} (orchestrate did not record runs)")

    # Pick target tenant/workorder from billing-state (source of truth).
    target = None
    for r in runs_all:
        tid = r.get("tenant_id", "")
        wid = r.get("work_order_id", "")
        if TENANT_ID_RE.match(tid) and wid:
            target = (tid, wid)
            break
    if not target:
        _die("Could not determine (tenant_id, work_order_id) from module_runs_log.csv" )
    tenant_id, work_order_id = target

    wo_dir = wo_root / tenant_id / work_order_id
    if not wo_dir.exists():
        # Print top-level diagnostics
        tenants = sorted([p.name for p in wo_root.iterdir() if p.is_dir()])[:20]
        _die(
            f"Runtime workorder folder not found: {wo_dir}. " 
            f"Available tenants under runtime/workorders (first 20): {tenants}"
        )

    runs = [r for r in runs_all if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id]
    if not runs:
        _die(f"No module runs found in {runs_path} for tenant={tenant_id} work_order={work_order_id}")

    for r in runs:
        mid = r.get("module_id", "")
        status = r.get("status", "")
        reason = r.get("reason_code", "")
        mr_id = r.get("module_run_id", "")

        if not MODULE_ID_RE.match(mid):
            _die(f"module_runs_log.csv invalid module_id for runtime verification: {mid!r}")

        if status == "COMPLETED":
            out_dir = _find_module_output_dir(wo_dir, mid, mr_id)
            if out_dir is None:
                subdirs = _dump_wo_dirs(wo_dir)
                _die(
                    "Missing runtime output folder for completed module "
                    f"{mid}: expected under {wo_dir} (tried common patterns, legacy variants, and id search). "
                    f"Workorder subdirs: {subdirs}"
                )
            files = _iter_files_recursive(out_dir)
            if not files:
                _die(f"Runtime output folder for module {mid} is empty: {out_dir}")
            nonempty = [p for p in files if p.stat().st_size > 0]
            if not nonempty:
                _die(f"Runtime output folder for module {mid} contains only empty files: {out_dir}")
        else:
            if not reason:
                _die(f"module_runs_log.csv: non-COMPLETED run must include reason_code (module {mid}, status {status!r})")

    _ok("Runtime outputs: validated against billing-state module_runs_log.csv (targeted + legacy variants)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["pre", "post", "release"], required=True)
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    args = ap.parse_args()

    repo_root = _repo_root()
    billing_state_dir = Path(args.billing_state_dir).resolve()
    runtime_dir = Path(args.runtime_dir).resolve()

    _verify_platform_billing(repo_root)
    _verify_maintenance_state(repo_root)
    _verify_dependency_index(repo_root)

    if args.phase in ("post", "release"):
        _verify_billing_state_dir(billing_state_dir)
        if args.phase == "post":
            _verify_runtime_outputs(runtime_dir, billing_state_dir)

    _ok(f"{args.phase.upper()} verification complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
