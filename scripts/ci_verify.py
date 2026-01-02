#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import re

import yaml

MODULE_ID_RE = re.compile(r"^\d{6}$")
TENANT_ID_RE = re.compile(r"^\d{10}$")


def _die(msg: str) -> None:
    print(f"[CI_VERIFY][FAIL] {msg}", file=sys.stderr)
    raise SystemExit(2)


def _warn(msg: str) -> None:
    print(f"[CI_VERIFY][WARN] {msg}")


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

    # Coverage: every canonical module folder must have at least one effective active price row.
    module_ids = _collect_module_ids(repo_root / "modules")
    by_mid: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        by_mid.setdefault(r.get("module_id", ""), []).append(r)

    today = date.today()

    def _parse_date(s: str) -> Optional[date]:
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
        _die(
            "platform/billing/module_prices.csv missing effective active price rows for modules: "
            + ", ".join(missing)
        )

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
    module_ids = set(_collect_module_ids(repo_root / "modules"))

    seen_ids: set[str] = set()
    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        if mid:
            if not MODULE_ID_RE.match(mid):
                _die(f"module_dependency_index.csv invalid module_id: {mid!r}")
            seen_ids.add(mid)
        raw = str(r.get("depends_on_module_ids", "")).strip()
        if not raw:
            continue
        # depends_on_module_ids is stored as JSON (list) in CSV.
        deps: List[str] = []
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                deps = [str(x) for x in v]
        except Exception:
            # tolerate legacy formatting like "[]" or comma-separated
            if raw.startswith("[") and raw.endswith("]"):
                pass
            else:
                deps = [x.strip() for x in raw.split(",") if x.strip()]

        for d in deps:
            if d and not MODULE_ID_RE.match(d):
                _die(f"module_dependency_index.csv invalid depends_on_module_id: {d!r} (module {mid})")
            if d and d not in module_ids:
                _die(f"module_dependency_index.csv dependency not found in modules/: {d} (required by {mid})")

    missing = sorted(module_ids - seen_ids)
    if missing:
        _die("module_dependency_index.csv missing module rows for: " + ", ".join(missing))
    _ok("Dependency index: coverage + referential integrity OK")


def _load_module_schema(repo_root: Path, module_id: str) -> Optional[Dict[str, Any]]:
    """Load tenant_params.schema.json either from modules/... or platform/schemas/..."""
    p1 = repo_root / "modules" / module_id / "tenant_params.schema.json"
    p2 = repo_root / "platform" / "schemas" / "work_order_modules" / f"{module_id}.schema.json"
    for p in (p1, p2):
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                return None
    return None


def _verify_platform_registries(repo_root: Path) -> None:
    module_ids = _collect_module_ids(repo_root / "modules")

    modules_csv = repo_root / "platform" / "modules" / "modules.csv"
    req_csv = repo_root / "platform" / "modules" / "requirements.csv"
    err_csv = repo_root / "platform" / "errors" / "error_reasons.csv"

    _assert_exact_header(modules_csv, ["module_id", "module_name", "version", "folder", "entrypoint", "description"])
    _assert_exact_header(req_csv, ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"])
    _assert_exact_header(err_csv, ["module_id", "error_code", "severity", "description", "remediation"])

    # Ensure every module is present in modules.csv.
    rows = _read_csv_rows(modules_csv)
    seen = {r.get("module_id", "") for r in rows}
    missing = [mid for mid in module_ids if mid not in seen]
    if missing:
        _die("platform/modules/modules.csv missing module rows for: " + ", ".join(missing))

    # Ensure schema sync coverage (if module defines tenant params).
    schemas_dir = repo_root / "platform" / "schemas" / "work_order_modules"
    for mid in module_ids:
        src = repo_root / "modules" / mid / "tenant_params.schema.json"
        if not src.exists():
            continue
        dst = schemas_dir / f"{mid}.schema.json"
        if not dst.exists():
            _die(f"Missing platform schema for module {mid}: {dst} (Maintenance must sync)")

    _ok("Platform registries: modules/requirements/errors + schemas coverage OK")


def _verify_workorders_scaffolded(repo_root: Path) -> None:
    """After Maintenance, every Work Order module entry must include scaffolded inputs."""
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        return

    for tenant_dir in sorted(tenants_dir.iterdir()):
        if not tenant_dir.is_dir() or not TENANT_ID_RE.match(tenant_dir.name):
            continue
        wo_dir = tenant_dir / "workorders"
        if not wo_dir.exists():
            continue

        for wf in sorted(wo_dir.glob("*.yml")):
            try:
                data = yaml.safe_load(wf.read_text(encoding="utf-8")) or {}
            except Exception as e:
                _die(f"Invalid YAML in workorder {wf}: {e}")

            mods = data.get("modules")
            if not isinstance(mods, list):
                continue

            for m in mods:
                if not isinstance(m, dict):
                    continue
                mid = str(m.get("module_id", "")).strip()
                if not mid:
                    continue
                if not MODULE_ID_RE.match(mid):
                    _die(f"Workorder {wf} has non-canonical module_id {mid!r} (expected 6 digits)")

                schema = _load_module_schema(repo_root, mid)
                if not schema:
                    continue

                props = schema.get("properties")
                if not isinstance(props, dict) or not props:
                    continue

                inputs = m.get("inputs")
                if not isinstance(inputs, dict):
                    _die(f"Workorder {wf} missing module inputs for module {mid} (Maintenance must scaffold)")

                missing_keys = [k for k in props.keys() if k not in inputs]
                if missing_keys:
                    _die(
                        f"Workorder {wf} module {mid} missing input keys (Maintenance must scaffold): "
                        + ", ".join(missing_keys)
                    )

    _ok("Workorders: module inputs scaffolded from tenant_params schemas")


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
    if not MODULE_ID_RE.match(mid6):
        return [mid6]
    n = int(mid6)
    return [mid6, f"{n:03d}", str(n)]


def _find_module_output_dir(wo_dir: Path, module_id_6: str, module_run_id: str) -> Optional[Path]:
    variants = _mid_variants(module_id_6)
    direct: List[Path] = []
    for v in variants:
        direct.extend([
            wo_dir / f"module-{v}",
            wo_dir / f"module_{v}",
            wo_dir / v,
            wo_dir / f"output-{v}",
            wo_dir / f"output_{v}",
            wo_dir / "modules" / v,
            wo_dir / "modules" / f"module-{v}",
        ])
    for p in direct:
        if p.exists() and p.is_dir():
            return p

    children = [p for p in wo_dir.iterdir() if p.is_dir()]
    for p in children:
        if any(v in p.name for v in variants) or (module_run_id and module_run_id in p.name):
            return p

    for p in wo_dir.rglob("*"):
        if p.is_dir() and (any(v in p.name for v in variants) or (module_run_id and module_run_id in p.name)):
            return p
    return None


def _dump_wo_dirs(wo_dir: Path) -> str:
    if not wo_dir.exists():
        return "<missing>"
    names = sorted([p.name for p in wo_dir.iterdir() if p.is_dir()])
    return ", ".join(names) if names else "<no subdirs>"


def _is_reuse(r: Dict[str, str]) -> bool:
    rot = (r.get("reuse_output_type") or "").strip().upper()
    if rot and rot not in ("NONE", "NO", "NEW", "GENERATED"):
        return True
    if (r.get("reuse_reference") or "").strip():
        return True
    if (r.get("cache_key_used") or "").strip():
        return True
    if (r.get("published_release_tag") or "").strip():
        return True
    if (r.get("release_manifest_name") or "").strip():
        return True
    return False


def _verify_runtime_outputs(runtime_dir: Path, billing_state_dir: Path) -> None:
    wo_root = runtime_dir / "workorders"
    if not wo_root.exists():
        _die(f"Missing runtime/workorders folder: {wo_root}")

    runs_path = billing_state_dir / "module_runs_log.csv"
    runs_all = _read_csv_rows(runs_path)
    if not runs_all:
        _die(f"module_runs_log.csv has no rows in {runs_path} (orchestrate did not record runs)")

    target = None
    for r in runs_all:
        tid = r.get("tenant_id", "")
        wid = r.get("work_order_id", "")
        if TENANT_ID_RE.match(tid) and wid:
            target = (tid, wid)
            break
    if not target:
        _die("Could not determine (tenant_id, work_order_id) from module_runs_log.csv")
    tenant_id, work_order_id = target

    wo_dir = wo_root / tenant_id / work_order_id
    if not wo_dir.exists():
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
                if _is_reuse(r):
                    _warn(
                        f"Completed module {mid} has no runtime output dir under {wo_dir} but reuse markers present "
                        f"(reuse_output_type={r.get('reuse_output_type')!r}, reuse_reference={r.get('reuse_reference')!r}, "
                        f"cache_key_used={r.get('cache_key_used')!r}, release_tag={r.get('published_release_tag')!r})."
                    )
                    continue

                subdirs = _dump_wo_dirs(wo_dir)
                _die(
                    "Missing runtime output folder for completed module "
                    f"{mid}: expected under {wo_dir} (tried common patterns, legacy variants, and id search). "
                    f"Workorder subdirs: {subdirs}. "
                    f"If orchestrator reuses outputs, it must populate reuse_* or cache_key_used fields."
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

    _ok("Runtime outputs: validated against module_runs_log.csv (reuse-aware + flexible folder naming)")


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
    _verify_platform_registries(repo_root)
    _verify_workorders_scaffolded(repo_root)

    if args.phase in ("post", "release"):
        _verify_billing_state_dir(billing_state_dir)
        if args.phase == "post":
            _verify_runtime_outputs(runtime_dir, billing_state_dir)

    _ok(f"{args.phase.upper()} verification complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
