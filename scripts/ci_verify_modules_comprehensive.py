from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

MODULE_ID_RE = re.compile(r"^(\d{3})_")
TEXT_EXTS = {".py", ".yaml", ".yml", ".json", ".md", ".txt"}

REQUIRED_FILES = [
    "module.yaml",
    "tenant_params.schema.json",
    "schema/results.schema.json",
    "schema/report.schema.json",
]

def _fail(msg: str) -> int:
    print(f"[CI_VERIFY][FAIL] {msg}")
    return 2

def _ok(msg: str) -> None:
    print(f"[CI_VERIFY][OK] {msg}")

def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])

def _assert_no_placeholders(mod_dir: Path) -> int:
    for p in mod_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in TEXT_EXTS:
            try:
                t = p.read_text(encoding="utf-8")
            except Exception:
                continue
            if "__MODULE_ID__" in t or "__MODULE_PREFIX__" in t:
                return _fail(f"Placeholders not rewritten in {p}")
    return 0

def _get_module_id_from_folder(mod_dir: Path) -> str:
    m = MODULE_ID_RE.match(mod_dir.name)
    if not m:
        raise ValueError("missing module id prefix")
    return m.group(1)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--schema-registry-dir", default="platform/schemas/work_order_modules")
    ap.add_argument("--module-catalog-path", default="platform/modules/modules.csv")
    ap.add_argument("--requirements-path", default="platform/modules/requirements.csv")
    ap.add_argument("--prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--error-reasons-path", default="platform/errors/error_reasons.csv")
    ap.add_argument("--maintenance-script", default="scripts/maintenance_modules.py")
    ap.add_argument("--maintenance-report-path", default="runtime/maintenance_modules_report.json")
    ap.add_argument("--work-orders-dir", default="")  # optional
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    if not modules_dir.exists():
        return _fail(f"Modules dir not found: {modules_dir}")

    # 1) Run helper (Maintenance) and require a report file.
    cmd = [
        sys.executable, args.maintenance_script,
        "--modules-dir", args.modules_dir,
        "--schema-registry-dir", args.schema_registry_dir,
        "--module-catalog-path", args.module_catalog_path,
        "--requirements-path", args.requirements_path,
        "--prices-path", args.prices_path,
        "--error-reasons-path", args.error_reasons_path,
        "--report-path", args.maintenance_report_path
    ]
    if args.work_orders_dir:
        cmd += ["--work-orders-dir", args.work_orders_dir]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        return _fail(f"Maintenance helper failed: {e}")

    report_path = Path(args.maintenance_report_path)
    if not report_path.exists():
        return _fail(f"Maintenance report missing: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    renamed = report.get("renamed_modules") or []

    # 2) Verify: no unassigned module folders remain
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        if not MODULE_ID_RE.match(mod_dir.name):
            return _fail(f"Unassigned module folder still present (should have been renamed): {mod_dir.name}")

    _ok("All module folders have numeric 3-digit prefixes")

    # 3) Verify required files + placeholders eliminated + module.yaml id matches folder
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)

        for rel in REQUIRED_FILES:
            if not (mod_dir / rel).exists():
                return _fail(f"Missing required file in {mod_dir.name}: {rel}")

        rc = _assert_no_placeholders(mod_dir)
        if rc != 0:
            return rc

        # module.yaml validation (minimal): contains module.id == mid
        text = (mod_dir / "module.yaml").read_text(encoding="utf-8")
        if f"id: '{mid}'" not in text and f'id: "{mid}"' not in text and f"id: {mid}" not in text:
            return _fail(f"module.yaml id does not match folder id for {mod_dir.name} (expected {mid})")

        # secrets env names must be mid_*
        if f"{mid}_GOOGLE_SEARCH_API_KEY" not in text or f"{mid}_GOOGLE_SEARCH_ENGINE_ID" not in text:
            return _fail(f"module.yaml secrets env names not normalized with module id prefix for {mod_dir.name}")

    _ok("Module files present; placeholders eliminated; module.yaml normalized")

    # 4) Verify schema registry contains <id>.schema.json for each module
    registry_dir = Path(args.schema_registry_dir)
    if not registry_dir.exists():
        return _fail(f"Schema registry dir missing: {registry_dir}")
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)
        if not (registry_dir / f"{mid}.schema.json").exists():
            return _fail(f"Tenant schema not registered for module {mid} at {registry_dir}/{mid}.schema.json")

    _ok("Tenant schemas registered for all modules")

    # 5) Verify platform tables updated: modules catalog, prices, error reasons, requirements
    cat_fields, cat_rows = _read_csv(Path(args.module_catalog_path))
    if not cat_rows:
        return _fail(f"Module catalog is empty or missing: {args.module_catalog_path}")

    price_fields, price_rows = _read_csv(Path(args.prices_path))
    if not price_rows:
        return _fail(f"Module prices table is empty or missing: {args.prices_path}")

    err_fields, err_rows = _read_csv(Path(args.error_reasons_path))
    if not err_rows:
        return _fail(f"Error reasons table is empty or missing: {args.error_reasons_path}")

    req_fields, req_rows = _read_csv(Path(args.requirements_path))
    if not req_rows:
        return _fail(f"Module requirements table is empty or missing: {args.requirements_path}")

    cat_by_id = {r.get("module_id",""): r for r in cat_rows}
    price_by_id = {r.get("module_id",""): r for r in price_rows}

    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)

        if mid not in cat_by_id:
            return _fail(f"Module {mid} missing from catalog table: {args.module_catalog_path}")
        if mid not in price_by_id:
            return _fail(f"Module {mid} missing from prices table: {args.prices_path}")

        # requirements: at least one secret requirement entry
        req_for_mid = [r for r in req_rows if r.get("module_id","")==mid]
        if not req_for_mid:
            return _fail(f"Module {mid} has no requirements rows in {args.requirements_path}")
        if not any(r.get("requirement_type","")=="secret" for r in req_for_mid):
            return _fail(f"Module {mid} requirements missing secret entries in {args.requirements_path}")

        # error reasons: at least one error code entry
        err_for_mid = [r for r in err_rows if r.get("module_id","")==mid]
        if not err_for_mid:
            return _fail(f"Module {mid} has no error reasons rows in {args.error_reasons_path}")

    _ok("Platform module catalog, prices, requirements, and error reasons include all modules")

    # 6) Verify helper actually renamed something if unassigned dirs existed (sanity)
    # If repo had an unassigned directory at start, report should include it; this is not always applicable.
    if renamed:
        _ok(f"Maintenance renamed {len(renamed)} module folder(s) as expected")

    _ok("Comprehensive module maintenance verification passed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
