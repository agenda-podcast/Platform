#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
# If the stdlib 'platform' module is loaded, remove it so our package can import
if 'platform' in sys.modules and not hasattr(sys.modules['platform'], '__path__'):
    del sys.modules['platform']

from platform.common.id_policy import validate_id
from platform.utils.csvio import read_csv, require_headers


BASE62_RE = re.compile(r"^[0-9A-Za-z]+$")


def _fail(msg: str) -> None:
    print(f"[CI_VERIFY][FAIL] {msg}")
    raise SystemExit(2)


def _ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")


def _warn(msg: str) -> None:
    print(f"[CI_VERIFY][WARN] {msg}")


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _assert_exact_header(path: Path, expected: List[str]) -> None:
    rows = read_csv(path)
    require_headers(path, expected)
    # Ensure exact (no extra columns) by reading header line directly
    header_line = path.read_text(encoding="utf-8").splitlines()[0]
    got = header_line.split(",")
    if got != expected:
        _fail(f"CSV header mismatch: file: {path} expected: {expected} got: {got}")


def _validate_repo_billing_config(repo_root: Path) -> None:
    billing = repo_root / "platform" / "billing"
    _assert_exact_header(billing / "billing_defaults.csv", ["key","value","notes"])
    _assert_exact_header(billing / "module_prices.csv", ["module_id","price_run_credits","price_save_to_release_credits","effective_from","effective_to","active","notes"])
    _assert_exact_header(billing / "promotions.csv", ["promo_id","code","type","value_credits","max_uses_per_tenant","valid_from","valid_to","active","rules_json","notes"])
    _assert_exact_header(billing / "topup_instructions.csv", ["topup_method_id","name","enabled","instructions"])
    _assert_exact_header(billing / "payments.csv", ["payment_id","tenant_id","topup_method_id","amount_credits","reference","received_at","status","note"])

    # ID format checks (static repo config)
    rows = read_csv(billing / "module_prices.csv")
    for r in rows:
        mid = str(r.get("module_id","")).strip()
        if mid:
            validate_id("module_id", mid, "module_prices.module_id")

    rows = read_csv(billing / "topup_instructions.csv")
    for r in rows:
        tid = str(r.get("topup_method_id","")).strip()
        if tid:
            validate_id("topup_method_id", tid, "topup_method_id")

    rows = read_csv(billing / "payments.csv")
    for r in rows:
        pid = str(r.get("payment_id","")).strip()
        if pid:
            validate_id("payment_id", pid, "payment_id")
        tenant_id = str(r.get("tenant_id","")).strip()
        if tenant_id:
            validate_id("tenant_id", tenant_id, "tenant_id")
        tm = str(r.get("topup_method_id","")).strip()
        if tm:
            validate_id("topup_method_id", tm, "topup_method_id")

    _ok("Repo billing config: headers + ID format basic validation OK")


def _validate_modules(repo_root: Path) -> None:
    modules_dir = repo_root / "modules"
    if not modules_dir.exists():
        _fail("modules/ directory missing")

    module_ids = set()
    for d in sorted(modules_dir.iterdir(), key=lambda p: p.name):
        if not d.is_dir():
            continue
        mid = d.name.strip()
        try:
            validate_id("module_id", mid, "module_id")
        except Exception as e:
            _fail(f"Invalid module folder name: {mid!r}: {e}")
        module_ids.add(mid)

        myml = d / "module.yml"
        if not myml.exists():
            _fail(f"Missing module.yml for module {mid}")
        cfg = _read_yaml(myml)
        declared = str(cfg.get("module_id","")).strip()
        if declared and declared != mid:
            _fail(f"module.yml module_id mismatch for {mid}: declared={declared!r}")
        deps = [str(x).strip() for x in (cfg.get("depends_on") or []) if str(x).strip()]
        for dep in deps:
            try:
                validate_id("module_id", dep, "depends_on")
            except Exception as e:
                _fail(f"Invalid depends_on in module {mid}: {dep!r}: {e}")

    # platform/modules/modules.csv must match folders
    pm = repo_root / "platform" / "modules" / "modules.csv"
    _assert_exact_header(pm, ["module_id","module_name","version","folder","entrypoint","description"])
    rows = read_csv(pm)
    for r in rows:
        mid = str(r.get("module_id","")).strip()
        if not mid:
            continue
        validate_id("module_id", mid, "platform/modules/modules.csv module_id")
        folder = str(r.get("folder","")).strip()
        if folder and folder != mid:
            _fail(f"modules.csv folder mismatch: module_id={mid!r} folder={folder!r}")
        if mid not in module_ids:
            _fail(f"modules.csv references missing module folder: {mid!r}")

    _ok("Modules: folder IDs + module.yml + modules.csv OK")


def _validate_tenants_and_workorders(repo_root: Path) -> None:
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    for td in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not td.is_dir():
            continue
        tid = td.name.strip()
        validate_id("tenant_id", tid, "tenant_id")
        tyml = td / "tenant.yml"
        if not tyml.exists():
            _fail(f"Missing tenant.yml for tenant {tid}")
        cfg = _read_yaml(tyml)
        declared = str(cfg.get("tenant_id","")).strip()
        if declared and declared != tid:
            _fail(f"tenant.yml tenant_id mismatch: folder={tid!r} declared={declared!r}")

        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            wo = _read_yaml(wp)
            wid = str(wo.get("work_order_id", wp.stem)).strip()
            validate_id("work_order_id", wid, "work_order_id")
            if wp.stem != wid:
                _fail(f"Workorder filename mismatch: {wp.name} declared work_order_id={wid!r}")

            # Workorder can be defined either as a simple module list, or as a steps-based
            # chaining plan (IFTTT-like wiring).
            steps = wo.get("steps")
            if steps is not None:
                if not isinstance(steps, list):
                    _fail(f"Invalid workorder steps list in {wp}")

                step_ids: List[str] = []
                step_id_set = set()
                from_refs: List[str] = []

                def _walk_refs(v: Any) -> None:
                    if isinstance(v, dict):
                        if "from_step" in v:
                            fr = str(v.get("from_step", "")).strip()
                            if fr:
                                from_refs.append(fr)
                        for vv in v.values():
                            _walk_refs(vv)
                    elif isinstance(v, list):
                        for vv in v:
                            _walk_refs(vv)

                for s in steps:
                    if not isinstance(s, dict):
                        _fail(f"Invalid step entry in {wp}: expected mapping")
                    sid = str(s.get("step_id", "")).strip()
                    if not sid or not re.match(r"^[0-9A-Za-z][0-9A-Za-z_-]{0,31}$", sid):
                        _fail(f"Invalid step_id {sid!r} in {wp}")
                    if sid in step_id_set:
                        _fail(f"Duplicate step_id {sid!r} in {wp}")
                    step_id_set.add(sid)
                    step_ids.append(sid)

                    mid = str(s.get("module_id", "")).strip()
                    validate_id("module_id", mid, "workorder.step.module_id")

                    inputs = s.get("inputs") or {}
                    if not isinstance(inputs, dict):
                        _fail(f"Invalid step.inputs in {wp}: step_id={sid!r}")
                    _walk_refs(inputs)

                for fr in from_refs:
                    if fr not in step_id_set:
                        _fail(f"Invalid binding from_step {fr!r} in {wp}: not in steps")

            mods = wo.get("modules") or []
            if steps is None:
                if not isinstance(mods, list):
                    _fail(f"Invalid workorder modules list in {wp}")
                for m in mods:
                    mid = str((m or {}).get("module_id", "")).strip()
                    validate_id("module_id", mid, "workorder.module_id")
            else:
                # If both are present, we still validate the module ids (but the runner will use steps).
                if not isinstance(mods, list):
                    _fail(f"Invalid workorder modules list in {wp}")
                for m in mods:
                    mid = str((m or {}).get("module_id", "")).strip()
                    if mid:
                        validate_id("module_id", mid, "workorder.module_id")

    _ok("Tenants + workorders: IDs + filenames OK")


def _validate_maintenance_state(repo_root: Path) -> None:
    ms = repo_root / "maintenance-state"
    required = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "module_dependency_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "platform_policy.csv",
        "maintenance_manifest.csv",
        "ids/category_registry.csv",
        "ids/reason_registry.csv",
    ]
    for rel in required:
        p = ms / rel
        if not p.exists():
            _fail(f"Missing maintenance-state file: {rel}")

    # spot-check IDs in reason catalog/registry
    cat = read_csv(ms / "reason_catalog.csv")
    for r in cat:
        rc = str(r.get("reason_code","")).strip()
        if rc:
            validate_id("reason_code", rc, "reason_code")
        rk = str(r.get("reason_key","")).strip()
        if rk:
            validate_id("reason_key", rk, "reason_key")
        scope = str(r.get("scope","")).strip().upper()
        if scope not in ("GLOBAL","MODULE"):
            _fail(f"Invalid reason scope: {scope!r}")
        mid = str(r.get("module_id","")).strip()
        if scope == "MODULE":
            validate_id("module_id", mid, "module_id")
        elif mid:
            _fail("Global reason has non-empty module_id")

    dep = read_csv(ms / "module_dependency_index.csv")
    for r in dep:
        validate_id("module_id", str(r.get("module_id","")).strip(), "module_dependency_index.module_id")
        validate_id("module_id", str(r.get("depends_on_module_id","")).strip(), "module_dependency_index.depends_on_module_id")

    _ok("Maintenance-state: required files + ID format OK")


def _validate_billing_state(billing_state_dir: Path) -> None:
    required_files = [
        "tenants_credits.csv",
        "transactions.csv",
        "transaction_items.csv",
        "promotion_redemptions.csv",
        "cache_index.csv",
        "workorders_log.csv",
        "module_runs_log.csv",
        "github_releases_map.csv",
        "github_assets_map.csv",
        "state_manifest.json",
    ]
    for fn in required_files:
        p = billing_state_dir / fn
        if not p.exists():
            _fail(f"Billing-state missing required file: {p}")

    # headers
    _assert_exact_header(billing_state_dir / "tenants_credits.csv", ["tenant_id","credits_available","updated_at","status"])
    _assert_exact_header(billing_state_dir / "transactions.csv", ["transaction_id","tenant_id","work_order_id","type","amount_credits","created_at","reason_code","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "transaction_items.csv", ["transaction_item_id","transaction_id","tenant_id","module_id","feature","type","amount_credits","created_at","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "promotion_redemptions.csv", ["redemption_id","tenant_id","promo_code","credits_granted","created_at","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "cache_index.csv", ["cache_key","tenant_id","module_id","created_at","expires_at","cache_id"])
    _assert_exact_header(billing_state_dir / "workorders_log.csv", ["work_order_id","tenant_id","status","created_at","started_at","ended_at","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "module_runs_log.csv", ["module_run_id","tenant_id","work_order_id","module_id","status","created_at","started_at","ended_at","reason_code","report_path","output_ref","metadata_json"])
    _assert_exact_header(billing_state_dir / "github_releases_map.csv", ["release_id","github_release_id","tag","tenant_id","work_order_id","created_at"])
    _assert_exact_header(billing_state_dir / "github_assets_map.csv", ["asset_id","github_asset_id","release_id","asset_name","created_at"])

    # ID format sanity on non-empty rows
    for r in read_csv(billing_state_dir / "tenants_credits.csv"):
        tid = str(r.get("tenant_id","")).strip()
        if tid:
            validate_id("tenant_id", tid, "tenant_id")

    for r in read_csv(billing_state_dir / "transactions.csv"):
        if r.get("transaction_id"):
            validate_id("transaction_id", str(r["transaction_id"]).strip(), "transaction_id")
        if r.get("tenant_id"):
            validate_id("tenant_id", str(r["tenant_id"]).strip(), "tenant_id")
        wid = str(r.get("work_order_id","")).strip()
        if wid:
            validate_id("work_order_id", wid, "work_order_id")

    _ok("Billing-state: required assets + headers + basic ID format OK")


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["pre","post"], required=True)
    ap.add_argument("--billing-state-dir", default=".billing-state")
    ap.add_argument("--runtime-dir", default="runtime")
    args = ap.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    billing_state_dir = Path(args.billing_state_dir).resolve()

    if args.phase == "pre":
        _validate_repo_billing_config(repo_root)
        _validate_modules(repo_root)
        _validate_tenants_and_workorders(repo_root)
        _validate_maintenance_state(repo_root)
    else:
        _validate_billing_state(billing_state_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
