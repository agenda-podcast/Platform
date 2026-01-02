#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List


MODULE_ID_RE = __import__("re").compile(r"^\d{6}$")


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

    # Coverage: every module folder must have at least one effective active price row.
    module_ids = _collect_module_ids(repo_root / "modules")
    by_mid = {}
    for r in rows:
        by_mid.setdefault(r.get("module_id", ""), []).append(r)

    from datetime import date

    today = date.today()

    def _parse_date(s: str):
        s = (s or "").strip()
        if not s:
            return None
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    def _effective_now(r: Dict[str, str]) -> bool:
        if str(r.get("active", "")).strip().lower() not in ("true", "1", "yes", "y"):
            return False
        ef = _parse_date(r.get("effective_from", ""))
        et = _parse_date(r.get("effective_to", ""))
        if ef and ef > today:
            return False
        if et and et < today:
            return False
        return True

    missing = []
    for mid in module_ids:
        if not any(_effective_now(r) for r in by_mid.get(mid, [])):
            missing.append(mid)
    if missing:
        _die(f"platform/billing/module_prices.csv missing effective active price rows for modules: {', '.join(missing)}")

    _ok("Repo billing config: headers + basic validation OK")


def _collect_module_ids(modules_dir: Path) -> List[str]:
    if not modules_dir.exists():
        _die("Missing modules/ folder")
    ids: List[str] = []
    bad: List[str] = []
    for p in sorted(modules_dir.iterdir()):
        if not p.is_dir():
            continue
        name = p.name.strip()
        if MODULE_ID_RE.match(name):
            ids.append(name)
        else:
            bad.append(name)
    if bad:
        _die(
            "Modules folder contains non-canonical module directories (Maintenance must rename them to 6-digit IDs): "
            + ", ".join(bad)
        )
    return ids


def _verify_platform_registries(repo_root: Path) -> None:
    module_ids = _collect_module_ids(repo_root / "modules")

    modules_csv = repo_root / "platform" / "modules" / "modules.csv"
    req_csv = repo_root / "platform" / "modules" / "requirements.csv"
    err_csv = repo_root / "platform" / "errors" / "error_reasons.csv"

    _assert_exact_header(modules_csv, ["module_id", "module_name", "version", "folder", "entrypoint", "description"])
    _assert_exact_header(req_csv, ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"])
    _assert_exact_header(err_csv, ["module_id", "error_code", "severity", "description", "remediation"])

    reg_rows = _read_csv_rows(modules_csv)
    reg_ids = {r.get("module_id", "") for r in reg_rows if r.get("module_id")}
    missing = [m for m in module_ids if m not in reg_ids]
    if missing:
        _die(f"platform/modules/modules.csv missing module rows for: {', '.join(missing)}")

    # Schemas: if a module declares tenant_params.schema.json, ensure it is synced into platform/schemas.
    schemas_dir = repo_root / "platform" / "schemas" / "work_order_modules"
    for mid in module_ids:
        src = repo_root / "modules" / mid / "tenant_params.schema.json"
        dst = schemas_dir / f"{mid}.schema.json"
        if src.exists() and not dst.exists():
            _die(f"Missing synced tenant schema: {dst} (source exists at {src})")

    _ok("Platform registries: modules/requirements/errors + schemas coverage OK")


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
        # Release-managed state (SoT for accounting)
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

    tenants = _read_csv_rows(billing_state_dir / "tenants_credits.csv")
    for t in tenants:
        ca = t.get("credits_available", "")
        if ca and not ca.isdigit():
            _die(f"tenants_credits.csv credits_available must be integer: {ca!r}")

    # Validate numeric fields in transactions and transaction_items
    txs = _read_csv_rows(billing_state_dir / "transactions.csv")
    for tx in txs:
        ta = tx.get("total_amount_credits", "")
        if ta and not ta.lstrip("-").isdigit():
            _die(f"transactions.csv total_amount_credits must be integer: {ta!r}")

    items = _read_csv_rows(billing_state_dir / "transaction_items.csv")
    for it in items:
        ac = it.get("amount_credits", "")
        if ac and not ac.lstrip("-").isdigit():
            _die(f"transaction_items.csv amount_credits must be integer: {ac!r}")

    _ok(f"Billing-state: required files + headers OK in {billing_state_dir}")


def _verify_runtime_outputs(runtime_dir: Path, billing_state_dir: Path) -> None:
    # E2E runtime output verification should be module-agnostic:
    # - For each module_run marked COMPLETED in billing-state, ensure its runtime output folder exists
    #   and contains at least one non-empty file.
    # - For FAILED runs, ensure a reason_code is present.
    tenant_id = "0000000001"
    work_order_id = "wo-2025-12-31-001"

    wo = runtime_dir / "workorders" / tenant_id / work_order_id
    if not wo.exists():
        _die(f"Missing runtime workorder folder: {wo}")

    runs_path = billing_state_dir / "module_runs_log.csv"
    rows = _read_csv_rows(runs_path)
    rows = [r for r in rows if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id]
    if not rows:
        _die(f"No module_runs_log rows for {tenant_id}/{work_order_id} in {runs_path}")

    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        status = str(r.get("status", "")).strip().upper()
        rc = str(r.get("reason_code", "")).strip()

        out_dir = wo / f"module-{mid}"
        if status == "COMPLETED":
            if not out_dir.exists():
                _die(f"Missing runtime output folder for module {mid}: {out_dir}")

            files = [p for p in out_dir.rglob("*") if p.is_file()]
            if not files:
                _die(f"Module {mid} completed but produced no runtime files in: {out_dir}")

            # Ensure at least one non-empty file exists
            if all(p.stat().st_size == 0 for p in files):
                _die(f"Module {mid} completed but all runtime files are empty in: {out_dir}")

        else:
            if not rc:
                _die(f"Module {mid} status={status} but reason_code is empty in module_runs_log.csv")

    _ok("Runtime outputs: verified for all COMPLETED module runs (non-empty files present); FAILED runs include reason_code")

def _verify_dependency_index(repo_root: Path) -> None:
    dep = repo_root / "maintenance-state" / "module_dependency_index.csv"
    rows = _read_csv_rows(dep)
    seen_002 = False
    for r in rows:
        if r.get("module_id") == "000002":
            seen_002 = True
            depends = r.get("depends_on_module_ids", "")
            if "000001" not in depends:
                _die("module_dependency_index.csv: module 000002 must depend on 000001")
    if not seen_002:
        _die("module_dependency_index.csv: missing module 000002 row")
    _ok("Dependency index: module 000002 depends on 000001")


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
    _verify_platform_registries(repo_root)
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
