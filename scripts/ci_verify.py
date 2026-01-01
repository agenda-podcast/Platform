#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List


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
        if mid and (len(mid) != 3 or not mid.isdigit()):
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


def _verify_billing_state_dir(billing_state_dir: Path) -> None:
    expected_headers = {
        "tenants_credits.csv": ["tenant_id", "credits_available", "updated_at"],
        "transactions.csv": ["transaction_id", "tenant_id", "type", "status", "created_at", "note"],
        "transaction_items.csv": [
            "transaction_item_id",
            "transaction_id",
            "tenant_id",
            "category",
            "name",
            "amount_credits",
            "created_at",
            "note",
        ],
        "promotion_redemptions.csv": [
            "redemption_id",
            "tenant_id",
            "promo_id",
            "code",
            "event_type",
            "transaction_item_id",
            "created_at",
            "note",
        ],
        "cache_index.csv": [
            "cache_key",
            "module_id",
            "tenant_id",
            "work_order_id",
            "artifact_relpath",
            "created_at",
            "hold_until",
            "status",
            "note",
        ],
        "workorders_log.csv": ["log_id", "tenant_id", "work_order_id", "status", "created_at", "note"],
        "module_runs_log.csv": [
            "log_id",
            "tenant_id",
            "work_order_id",
            "module_id",
            "module_run_id",
            "status",
            "reason_key",
            "created_at",
            "note",
        ],
    }

    for fname, hdr in expected_headers.items():
        _ensure_file(billing_state_dir / fname, hdr)
        _assert_exact_header(billing_state_dir / fname, hdr)

    tenants = _read_csv_rows(billing_state_dir / "tenants_credits.csv")
    for t in tenants:
        ca = t.get("credits_available", "")
        if ca and not ca.isdigit():
            _die(f"tenants_credits.csv credits_available must be integer: {ca!r}")

    _ok(f"Billing-state: required files + headers OK in {billing_state_dir}")


def _verify_runtime_outputs(runtime_dir: Path) -> None:
    wo = runtime_dir / "workorders" / "tenant-001" / "wo-2025-12-31-001"
    m1 = wo / "module-001" / "source_text.txt"
    m2 = wo / "module-002" / "derived_notes.txt"

    if not m1.exists():
        _die(f"Missing runtime output from module 001: {m1}")
    if not m2.exists():
        _die(f"Missing runtime output from module 002: {m2}")

    if m1.stat().st_size < 10:
        _die(f"module 001 output unexpectedly small: {m1} ({m1.stat().st_size} bytes)")
    if m2.stat().st_size < 10:
        _die(f"module 002 output unexpectedly small: {m2} ({m2.stat().st_size} bytes)")

    _ok("Runtime outputs: module-001/source_text.txt and module-002/derived_notes.txt present and non-trivial")


def _verify_dependency_index(repo_root: Path) -> None:
    dep = repo_root / "maintenance-state" / "module_dependency_index.csv"
    rows = _read_csv_rows(dep)
    seen_002 = False
    for r in rows:
        if r.get("module_id") == "002":
            seen_002 = True
            depends = r.get("depends_on_module_ids", "")
            if "001" not in depends:
                _die("module_dependency_index.csv: module 002 must depend on 001")
    if not seen_002:
        _die("module_dependency_index.csv: missing module 002 row")
    _ok("Dependency index: module 002 depends on 001")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["pre", "post", "release"], required=True)
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    billing_state_dir = Path(args.billing_state_dir).resolve()
    runtime_dir = Path(args.runtime_dir).resolve()

    _verify_platform_billing(repo_root)
    _verify_maintenance_state(repo_root)
    _verify_dependency_index(repo_root)

    if args.phase in ("post", "release"):
        _verify_billing_state_dir(billing_state_dir)
        if args.phase == "post":
            _verify_runtime_outputs(runtime_dir)

    _ok(f"{args.phase.upper()} verification complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
