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

EXPECTED_PRICES_HEADER = ["module_id","price_run_credits","price_save_to_release_credits","effective_from","effective_to","active","notes"]
EXPECTED_CATALOG_HEADER = ["module_id","module_name","version","folder","entrypoint","description"]
EXPECTED_REQ_HEADER = ["module_id","requirement_type","requirement_key","requirement_value","note"]
EXPECTED_ERR_HEADER = ["module_id","error_code","severity","description","remediation"]

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

def _assert_header(path: Path, expected: List[str]) -> int:
    header, _ = _read_csv(path)
    if header != expected:
        return _fail(f"CSV header mismatch: file {path} expected {expected} got {header}")
    return 0

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

    # 1) Run helper and require report
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

    # 2) No unassigned module folders remain
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        if not MODULE_ID_RE.match(mod_dir.name):
            return _fail(f"Unassigned module folder still present (should have been renamed): {mod_dir.name}")
    _ok("All module folders have numeric 3-digit prefixes")

    # 3) Required files + placeholders eliminated + module.yaml normalized
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)

        for rel in REQUIRED_FILES:
            if not (mod_dir / rel).exists():
                return _fail(f"Missing required file in {mod_dir.name}: {rel}")

        rc = _assert_no_placeholders(mod_dir)
        if rc != 0:
            return rc

        text = (mod_dir / "module.yaml").read_text(encoding="utf-8")
        if f"id: '{mid}'" not in text and f'id: "{mid}"' not in text and f"id: {mid}" not in text:
            return _fail(f"module.yaml id does not match folder id for {mod_dir.name} (expected {mid})")

        if f"{mid}_GOOGLE_SEARCH_API_KEY" not in text or f"{mid}_GOOGLE_SEARCH_ENGINE_ID" not in text:
            return _fail(f"module.yaml secrets env names not normalized with module id prefix for {mod_dir.name}")

    _ok("Module files present; placeholders eliminated; module.yaml normalized")

    # 4) Schema registry contains <id>.schema.json
    registry_dir = Path(args.schema_registry_dir)
    if not registry_dir.exists():
        return _fail(f"Schema registry dir missing: {registry_dir}")
    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)
        if not (registry_dir / f"{mid}.schema.json").exists():
            return _fail(f"Tenant schema not registered for module {mid} at {registry_dir}/{mid}.schema.json")
    _ok("Tenant schemas registered for all modules")

    # 5) Platform tables exist and headers match platform CI expectations
    for path, expected in [
        (Path(args.prices_path), EXPECTED_PRICES_HEADER),
        (Path(args.module_catalog_path), EXPECTED_CATALOG_HEADER),
        (Path(args.requirements_path), EXPECTED_REQ_HEADER),
        (Path(args.error_reasons_path), EXPECTED_ERR_HEADER),
    ]:
        rc = _assert_header(path, expected)
        if rc != 0:
            return rc
    _ok("Platform table headers match expected schemas")

    # 6) Each module id is present in tables
    _, cat_rows = _read_csv(Path(args.module_catalog_path))
    _, price_rows = _read_csv(Path(args.prices_path))
    _, req_rows = _read_csv(Path(args.requirements_path))
    _, err_rows = _read_csv(Path(args.error_reasons_path))

    cat_ids = {r.get("module_id","") for r in cat_rows}
    price_ids = {r.get("module_id","") for r in price_rows}
    req_ids = {r.get("module_id","") for r in req_rows}
    err_ids = {r.get("module_id","") for r in err_rows}

    for mod_dir in sorted([p for p in modules_dir.iterdir() if p.is_dir()], key=lambda p: p.name):
        mid = _get_module_id_from_folder(mod_dir)
        if mid not in cat_ids:
            return _fail(f"Module {mid} missing from catalog table: {args.module_catalog_path}")
        if mid not in price_ids:
            return _fail(f"Module {mid} missing from prices table: {args.prices_path}")
        if mid not in req_ids:
            return _fail(f"Module {mid} missing from requirements table: {args.requirements_path}")
        if mid not in err_ids:
            return _fail(f"Module {mid} missing from error reasons table: {args.error_reasons_path}")
    _ok("Platform tables include all modules")

    _ok("Comprehensive module maintenance verification passed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
