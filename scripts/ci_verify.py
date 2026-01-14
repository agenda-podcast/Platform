#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path as _Path

# Ensure repo root is on sys.path so local 'platform' package wins over stdlib 'platform' module
_REPO_ROOT = _Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

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
    _assert_exact_header(billing / "module_prices.csv", ["module_id","deliverable_id","price_credits","effective_from","effective_to","active","notes"])
    _assert_exact_header(billing / "promotions.csv", ["promo_id","code","type","value_credits","max_uses_per_tenant","valid_from","valid_to","active","rules_json","notes"])
    _assert_exact_header(billing / "topup_instructions.csv", ["topup_method_id","name","enabled","instructions"])
    _assert_exact_header(billing / "payments.csv", ["payment_id","tenant_id","topup_method_id","amount_credits","reference","received_at","status","note"])

    # ID format checks (static repo config)
    rows = read_csv(billing / "module_prices.csv")
    active_run_rows = set()
    deliverable_re = re.compile(r"^[A-Za-z0-9_]+$")

    for r in rows:
        mid = str(r.get("module_id","")).strip()
        did = str(r.get("deliverable_id","")).strip()
        if mid:
            validate_id("module_id", mid, "module_prices.module_id")
        if not did:
            _fail("module_prices.csv has empty deliverable_id")
        if did != "__run__" and not deliverable_re.match(did):
            _fail(f"module_prices.csv invalid deliverable_id: {did!r} (allowed: [A-Za-z0-9_]+ or '__run__')")

        active = str(r.get("active","") or "").strip().lower() == "true"
        if active and did == "__run__" and mid:
            active_run_rows.add(mid)

    # Ensure every module has an active __run__ price row
    modules_dir = repo_root / "modules"
    if modules_dir.exists():
        for d in modules_dir.iterdir():
            if not d.is_dir():
                continue
            mid = d.name.strip()
            if not mid:
                continue
            if mid not in active_run_rows:
                _fail(f"module_prices.csv missing active __run__ row for module: {mid}")


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
        # Dependencies are not allowed at the module layer. All wiring must be expressed in workorders via steps/bindings.
        if "depends_on" in cfg and (cfg.get("depends_on") not in (None, [], "")):
            _fail(f"Module {mid} defines depends_on, but module dependencies are not supported")

        ports = cfg.get("ports") or {}
        if not isinstance(ports, dict):
            _fail(f"Module {mid}: ports must be an object")
        p_in = ports.get("inputs") or {}
        p_out = ports.get("outputs") or {}
        if not isinstance(p_in, dict) or not isinstance(p_out, dict):
            _fail(f"Module {mid}: ports.inputs and ports.outputs must be objects")
        in_port = p_in.get("port") or []
        in_limited = p_in.get("limited_port") or []
        out_port = p_out.get("port") or []
        out_limited = p_out.get("limited_port") or []
        if not all(isinstance(x, list) for x in (in_port, in_limited, out_port, out_limited)):
            _fail(f"Module {mid}: ports.*.port and ports.*.limited_port must be lists")

        def _validate_port_list(lst: List[Any], kind: str) -> None:
            seen = set()
            for p in lst:
                if not isinstance(p, dict):
                    _fail(f"Module {mid}: invalid {kind} port (expected object)")
                pid = str(p.get("id", "")).strip()
                if not pid:
                    _fail(f"Module {mid}: {kind} port missing id")
                if pid in seen:
                    _fail(f"Module {mid}: duplicate {kind} port id {pid!r}")
                seen.add(pid)
                # For tenant-visible output ports, require non-empty path.
                if kind.startswith("outputs.port"):
                    path = str(p.get("path", "")).lstrip("/").strip()
                    if not path:
                        _fail(f"Module {mid}: tenant-visible output port {pid!r} must define non-empty path")

        _validate_port_list(in_port, "inputs.port")
        _validate_port_list(in_limited, "inputs.limited_port")
        _validate_port_list(out_port, "outputs.port")
        _validate_port_list(out_limited, "outputs.limited_port")

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

    module_ports_cache: Dict[str, Dict[str, Any]] = {}
    module_deliverables_cache: Dict[str, set[str]] = {}

    def _load_deliverables(mid: str) -> set[str]:
        mid = str(mid).strip()
        if mid in module_deliverables_cache:
            return module_deliverables_cache[mid]
        myml = repo_root / "modules" / mid / "module.yml"
        if not myml.exists():
            _fail(f"Workorder references missing module folder: {mid!r}")
        cfg = _read_yaml(myml)
        deliverables = cfg.get("deliverables") or {}
        port = []
        if isinstance(deliverables, dict):
            port = deliverables.get("port") or []
        if not isinstance(port, list):
            port = []
        s: set[str] = set()
        for d in port:
            if not isinstance(d, dict):
                continue
            did = str(d.get("deliverable_id", "")).strip()
            if did:
                s.add(did)
        module_deliverables_cache[mid] = s
        return s

    seen_workorders_global: Dict[str, str] = {}

    def _load_ports(mid: str) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
        """Return (tenant_inputs, platform_inputs, required_tenant_inputs, tenant_output_paths)."""
        mid = str(mid).strip()
        if mid in module_ports_cache:
            ports = module_ports_cache[mid]
        else:
            myml = repo_root / "modules" / mid / "module.yml"
            if not myml.exists():
                _fail(f"Workorder references missing module folder: {mid!r}")
            cfg = _read_yaml(myml)
            ports = cfg.get("ports") or {}
            if not isinstance(ports, dict):
                _fail(f"Module {mid}: ports must be an object")
            module_ports_cache[mid] = ports

        p_in = ports.get("inputs") or {}
        p_out = ports.get("outputs") or {}
        if not isinstance(p_in, dict) or not isinstance(p_out, dict):
            _fail(f"Module {mid}: ports.inputs/ports.outputs must be objects")
        in_port = p_in.get("port") or []
        in_limited = p_in.get("limited_port") or []
        out_port = p_out.get("port") or []
        out_limited = p_out.get("limited_port") or []
        if not all(isinstance(x, list) for x in (in_port, in_limited, out_port, out_limited)):
            _fail(f"Module {mid}: ports.*.port and ports.*.limited_port must be lists")

        tenant_inputs: Set[str] = set()
        platform_inputs: Set[str] = set()
        required_tenant: Set[str] = set()
        tenant_output_paths: Set[str] = set()

        for p in in_port:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id", "")).strip()
            if not pid:
                continue
            tenant_inputs.add(pid)
            if bool(p.get("required", False)):
                required_tenant.add(pid)

        for p in in_limited:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id", "")).strip()
            if not pid:
                continue
            platform_inputs.add(pid)

        for p in out_port:
            if not isinstance(p, dict):
                continue
            path = str(p.get("path", "")).lstrip("/").strip()
            if path:
                tenant_output_paths.add(path)

        return tenant_inputs, platform_inputs, required_tenant, tenant_output_paths

    def _is_binding(v: Any) -> bool:
        return isinstance(v, dict) and bool(v.get("from_step")) and bool(v.get("from_file"))

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

            # Global uniqueness: work_order_id must be unique across all tenants.
            if wid in seen_workorders_global and seen_workorders_global[wid] != tid:
                _fail(f"work_order_id must be globally unique: {wid!r} used by tenants {seen_workorders_global[wid]!r} and {tid!r}")
            seen_workorders_global[wid] = tid

            if "modules" in wo and (wo.get("modules") not in (None, [], "")):
                _fail(f"Legacy workorders with 'modules' are not supported: {wp}")

            # Steps-only workorders: all chaining/wiring is expressed here.
            if "modules" in wo and wo.get("modules") not in (None, [], ""):
                _fail(f"Legacy workorder 'modules' is not supported: {wp}")

            steps = wo.get("steps")
            if not (isinstance(steps, list) and steps):
                _fail(f"Workorder must define non-empty steps list: {wp}")

            step_id_set: Set[str] = set()
            step_to_module: Dict[str, str] = {}
            step_outputs: Dict[str, Set[str]] = {}

            # First pass: ids, module ids, input keys, required inputs
            for s in steps:
                if not isinstance(s, dict):
                    _fail(f"Invalid step entry in {wp}: expected mapping")
                sid = str(s.get("step_id", "")).strip()
                try:
                    validate_id("step_id", sid, "workorder.step.step_id")
                except Exception:
                    _fail(f"Invalid step_id {sid!r} in {wp} (expected Base62 length 2)")
                if sid in step_id_set:
                    _fail(f"Duplicate step_id {sid!r} in {wp}")
                step_id_set.add(sid)

                # Human-friendly name (must not be used for logic).
                sname = s.get("step_name", None)
                if sname is None:
                    sname = s.get("name", None)
                sname = "" if sname is None else str(sname)
                if not sname.strip():
                    _fail(f"Missing required step_name for step_id={sid!r} in {wp}")
                if "\n" in sname or "\r" in sname:
                    _fail(f"Invalid step_name (newline) for step_id={sid!r} in {wp}")
                if len(sname.strip()) > 80:
                    _fail(f"Invalid step_name (too long) for step_id={sid!r} in {wp} (max 80 chars)")

                mid = str(s.get("module_id", "")).strip()
                validate_id("module_id", mid, "workorder.step.module_id")

                # Milestone 8A: deliverables must be explicitly declared per step and exist in module contract.
                deliverables = s.get("deliverables")
                if not (isinstance(deliverables, list) and deliverables):
                    _fail(f"Workorder step must define non-empty deliverables list: {wp} step_id={sid!r} module_id={mid!r}")
                cleaned = [str(d).strip() for d in deliverables if str(d).strip()]
                if not cleaned:
                    _fail(f"Workorder step deliverables list is empty after normalization: {wp} step_id={sid!r} module_id={mid!r}")
                deliverable_re = re.compile(r"^[A-Za-z0-9_]+$")
                for did in cleaned:
                    if did == "__run__":
                        _fail(f"Workorder step deliverables must not include __run__: {wp} step_id={sid!r}")
                    if not deliverable_re.match(did):
                        _fail(f"Workorder step deliverables invalid id format: {did!r} in {wp} (allowed: [A-Za-z0-9_]+)")
                allowed = _load_deliverables(mid)
                if not allowed:
                    _fail(f"Module {mid} has no deliverables contract in module.yml, but workorder references deliverables: {wp}")
                for did in cleaned:
                    if did not in allowed:
                        _fail(f"Workorder references undefined deliverable {did!r} for module {mid} in {wp}")
                step_to_module[sid] = mid

                tenant_ins, platform_ins, required_ins, tenant_out_paths = _load_ports(mid)
                step_outputs[sid] = tenant_out_paths

                inputs = s.get("inputs") or {}
                if not isinstance(inputs, dict):
                    _fail(f"Invalid step.inputs in {wp}: step_id={sid!r}")

                for k in inputs.keys():
                    if k in platform_ins:
                        _fail(f"Step {sid!r} in {wp}: input {k!r} is platform-only for module {mid}")
                    if tenant_ins and k not in tenant_ins:
                        _fail(f"Step {sid!r} in {wp}: unknown input {k!r} for module {mid}")

                for req in required_ins:
                    if req not in inputs:
                        _fail(f"Step {sid!r} in {wp}: missing required input {req!r} for module {mid}")

            # Second pass: validate bindings (from_step existence + output exposure)
            def _walk_bindings(v: Any) -> None:
                if _is_binding(v):
                    fr = str(v.get("from_step", "")).strip()
                    ff = str(v.get("from_file", "")).lstrip("/").strip()
                    if fr not in step_id_set:
                        _fail(f"Invalid binding from_step {fr!r} in {wp}: not in steps")
                    allowed = step_outputs.get(fr) or set()
                    if ff and allowed and ff not in allowed:
                        _fail(f"Invalid binding from_file {ff!r} in {wp}: not exposed by step {fr!r}")
                if isinstance(v, dict):
                    for vv in v.values():
                        _walk_bindings(vv)
                elif isinstance(v, list):
                    for vv in v:
                        _walk_bindings(vv)

            for s in steps:
                inputs = s.get("inputs") or {}
                _walk_bindings(inputs)

    _ok("Tenants + workorders: IDs + filenames OK")


def _validate_maintenance_state(repo_root: Path) -> None:
    ms = repo_root / "maintenance-state"
    required = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "workorders_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "module_contract_rules.csv",
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
    _assert_exact_header(billing_state_dir / "transaction_items.csv", ["transaction_item_id","transaction_id","tenant_id","module_id","work_order_id","step_id","deliverable_id","feature","type","amount_credits","created_at","note","metadata_json"])
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
