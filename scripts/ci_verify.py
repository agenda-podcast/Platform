#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
import json
import yaml
from pathlib import Path
from typing import Dict, List

# Canonical formats (as per repo contract):
#   module_id: 6 digits (NNNNNN)
#   tenant_id: 10 digits (NNNNNNNNNN)
MODULE_ID_RE = re.compile(r"^\d{6}$")
TENANT_ID_RE = re.compile(r"^\d{10}$")


def _die(msg: str) -> None:
    print(f"[CI_VERIFY][FAIL] {msg}", file=sys.stderr)
    raise SystemExit(2)


def _ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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
        w = csv.writer(f)
        w.writerow(header)


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
        mid = (r.get("module_id") or "").strip()
        if mid and not MODULE_ID_RE.match(mid):
            _die(f"platform/billing/module_prices.csv invalid module_id at line {i}: {mid!r} (expected 6 digits)")

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
        ms / "platform_policy.csv",
        ms / "tenant_relationships.csv",
        ms / "ids" / "module_registry.csv",
    ]
    for p in required:
        if not p.exists():
            _die(f"Missing maintenance-state required file: {p}")

    _ok("Maintenance-state: required files present")


def _verify_module_artifacts_contract(repo_root: Path) -> None:
    """Ensure every module declares artifacts support and denial reasons."""
    modules_dir = repo_root / "modules"
    if not modules_dir.exists():
        _die("Missing modules/ folder")

    for p in sorted(modules_dir.iterdir()):
        if not p.is_dir():
            continue
        mid = p.name
        if not MODULE_ID_RE.match(mid):
            continue

        module_yml = p / "module.yml"
        if not module_yml.exists():
            _die(f"Missing module.yml for module {mid}")
        with module_yml.open("r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        if "supports_downloadable_artifacts" not in y:
            _die(f"Module {mid} missing supports_downloadable_artifacts in module.yml")

        vpath = p / "validation.yml"
        if not vpath.exists():
            _die(f"Module {mid} missing validation.yml")
        with vpath.open("r", encoding="utf-8") as f:
            vy = yaml.safe_load(f) or {}
        reasons = vy.get("reasons") or []
        keys = {str(r.get("reason_key", "")).strip() for r in reasons if isinstance(r, dict)}
        for req in ("artifacts_download_not_allowed_by_module", "artifacts_download_not_allowed_by_platform"):
            if req not in keys:
                _die(f"Module {mid} missing required reason_key in validation.yml: {req}")

    _ok("Modules: artifacts contract enforced (supports flag + denial reasons)")


def _parse_dep_list(raw: str) -> List[str]:
    """Parse depends_on_module_ids from CSV.

    Acceptable encodings:
      - empty / null-ish: "", "[]", "null", "none"  -> []
      - JSON list: ["000001","000002"]
      - pipe-separated: "000001|000002"
    """
    s = (raw or "").strip()
    if not s:
        return []
    low = s.lower()
    if low in ("[]", "null", "none", "nil", "n/a", "na"):
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
        except Exception:
            if low == "[]":
                return []
            raise
        if arr is None:
            return []
        if not isinstance(arr, list):
            return []
        out: List[str] = []
        for x in arr:
            if x is None:
                continue
            out.append(str(x).strip())
        return [x for x in out if x]
    return [x.strip() for x in s.split("|") if x.strip()]


def _verify_dependency_index(repo_root: Path) -> None:
    dep = repo_root / "maintenance-state" / "module_dependency_index.csv"
    rows = _read_csv_rows(dep)
    if not rows:
        _die("module_dependency_index.csv: no rows")

    for r in rows:
        mid = (r.get("module_id") or "").strip()
        if mid and not MODULE_ID_RE.match(mid):
            _die(f"module_dependency_index.csv invalid module_id: {mid!r} (expected 6 digits)")

        deps_raw = (r.get("depends_on_module_ids") or "").strip()
        try:
            deps = _parse_dep_list(deps_raw)
        except Exception:
            _die(f"module_dependency_index.csv depends_on_module_ids is not parseable: {deps_raw!r}")

        for d in deps:
            if not MODULE_ID_RE.match(d):
                _die(f"module_dependency_index.csv invalid depends_on_module_id: {d!r} (expected 6 digits)")

    _ok("Dependency index: format OK")


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

    tenants = _read_csv_rows(billing_state_dir / "tenants_credits.csv")
    for t in tenants:
        tid = (t.get("tenant_id") or "").strip()
        if tid and not TENANT_ID_RE.match(tid):
            _die(f"tenants_credits.csv invalid tenant_id: {tid!r} (expected 10 digits)")
        ca = (t.get("credits_available") or "").strip()
        if ca and not ca.isdigit():
            _die(f"tenants_credits.csv credits_available must be integer: {ca!r}")

    runs = _read_csv_rows(billing_state_dir / "module_runs_log.csv")
    for r in runs:
        mid = (r.get("module_id") or "").strip()
        if mid and not MODULE_ID_RE.match(mid):
            _die(f"module_runs_log.csv invalid module_id: {mid!r} (expected 6 digits)")
        tid = (r.get("tenant_id") or "").strip()
        if tid and not TENANT_ID_RE.match(tid):
            _die(f"module_runs_log.csv invalid tenant_id: {tid!r} (expected 10 digits)")

    _ok(f"Billing-state: required files + headers OK in {billing_state_dir}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["pre", "post", "release"], required=True)
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    args = ap.parse_args()

    repo_root = _repo_root()
    billing_state_dir = Path(args.billing_state_dir).resolve()

    _verify_platform_billing(repo_root)
    _verify_maintenance_state(repo_root)
    _verify_module_artifacts_contract(repo_root)
    _verify_dependency_index(repo_root)

    if args.phase in ("post", "release"):
        _verify_billing_state_dir(billing_state_dir)

    _ok(f"{args.phase.upper()} verification complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
