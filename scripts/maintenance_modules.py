from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # PyYAML
except Exception as e:
    yaml = None


MODULE_ID_RE = re.compile(r"^(\d{3})_")


# ----------------------------
# CSV helpers
# ----------------------------

def _read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return (reader.fieldnames or [], [dict(r) for r in reader])


def _write_csv_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def _upsert_rows(
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    key_fields: List[str],
    new_rows: List[Dict[str, str]]
) -> Tuple[List[Dict[str, str]], int]:
    idx: Dict[Tuple[str, ...], int] = {}
    for i, r in enumerate(rows):
        key = tuple(r.get(k, "") for k in key_fields)
        idx[key] = i

    changed = 0
    for nr in new_rows:
        key = tuple(nr.get(k, "") for k in key_fields)
        if key in idx:
            i = idx[key]
            merged = dict(rows[i])
            for fn in fieldnames:
                if fn in nr and nr[fn] != "":
                    merged[fn] = nr[fn]
            if merged != rows[i]:
                rows[i] = merged
                changed += 1
        else:
            # normalize to all fields
            rows.append({fn: nr.get(fn, "") for fn in fieldnames})
            changed += 1
    return rows, changed


# ----------------------------
# Module discovery + ID assignment
# ----------------------------

def list_module_dirs(modules_dir: Path) -> List[Path]:
    return [p for p in modules_dir.iterdir() if p.is_dir()]


def parse_used_ids(modules_dir: Path) -> List[int]:
    used: List[int] = []
    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if m:
            used.append(int(m.group(1)))
    return sorted(set(used))


def next_unused_min_id(used: List[int]) -> int:
    i = 1
    used_set = set(used)
    while i in used_set:
        i += 1
    return i


def is_unassigned_module_dir(p: Path) -> bool:
    return MODULE_ID_RE.match(p.name) is None


def rewrite_text_file(path: Path, replacements: Dict[str, str]) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return False

    new_text = text
    for k, v in replacements.items():
        new_text = new_text.replace(k, v)

    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        return True
    return False


def rewrite_placeholders_in_dir(module_dir: Path, module_id: str) -> int:
    replacements = {
        "__MODULE_ID__": module_id,
        "__MODULE_PREFIX__": f"{module_id}_",
    }

    changed = 0
    for path in module_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".py", ".yaml", ".yml", ".json", ".md", ".txt"}:
            if rewrite_text_file(path, replacements):
                changed += 1
    return changed


def assign_ids(modules_dir: Path) -> List[Dict[str, Any]]:
    """
    Assign IDs to any non-numeric module folder under modules_dir.
    Returns structured rename actions.
    """
    actions: List[Dict[str, Any]] = []
    used_ids = parse_used_ids(modules_dir)

    for p in sorted(list_module_dirs(modules_dir), key=lambda x: x.name):
        if not is_unassigned_module_dir(p):
            continue

        new_id_int = next_unused_min_id(used_ids)
        used_ids.append(new_id_int)
        used_ids.sort()

        module_id = f"{new_id_int:03d}"
        new_name = f"{module_id}_{p.name}"
        new_path = modules_dir / new_name

        if new_path.exists():
            raise RuntimeError(f"Target module dir already exists: {new_path}")

        old_name = p.name
        p.rename(new_path)
        rewritten_files = rewrite_placeholders_in_dir(new_path, module_id)

        actions.append({
            "old_folder": old_name,
            "new_folder": new_name,
            "module_id": module_id,
            "placeholders_rewritten_files_count": rewritten_files
        })

    return actions


# ----------------------------
# YAML module parsing + normalization
# ----------------------------

def _load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required for maintenance_modules.py (pip install pyyaml)")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _dump_yaml(data: Dict[str, Any]) -> str:
    if yaml is None:
        raise RuntimeError("PyYAML is required for maintenance_modules.py (pip install pyyaml)")
    # stable-ish, readable
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def normalize_module_yaml(module_dir: Path, module_id: str) -> Dict[str, Any]:
    """
    Enforce invariants inside module.yaml after placeholder rewrite:
      - module.id == module_id
      - entrypoint contains run_module_{module_id}
      - secrets env names use "{module_id}_..."
      - pricing / errors / requirements exist (fail-safe auto-add if missing)
    Returns dict with normalization actions.
    """
    module_yaml_path = module_dir / "module.yaml"
    data = _load_yaml(module_yaml_path)

    actions: Dict[str, Any] = {"module_id": module_id, "updated": False, "fixes": []}

    mod = data.get("module") or {}
    if mod.get("id") != module_id:
        mod["id"] = module_id
        actions["updated"] = True
        actions["fixes"].append("module.id")

    # entrypoint normalization
    entry = str(mod.get("entrypoint") or "")
    # expected suffix function name:
    expected_fn = f"run_module_{module_id}"
    if expected_fn not in entry:
        # Try replace run_module_XXX or run_module___MODULE_ID__
        entry = re.sub(r"run_module_\d{3}", expected_fn, entry)
        entry = entry.replace("run_module___MODULE_ID__", expected_fn)
        # If still not there, force format "google_search_pages:run_module_<id>" using module.name if available
        if expected_fn not in entry:
            module_name = mod.get("name") or module_dir.name.split("_", 1)[-1]
            entry = f"{module_name}:{expected_fn}"
        mod["entrypoint"] = entry
        actions["updated"] = True
        actions["fixes"].append("module.entrypoint")

    data["module"] = mod

    # secrets env normalization
    secrets = data.get("secrets") or {}
    api_key_env = secrets.get("api_key_env") or f"{module_id}_GOOGLE_SEARCH_API_KEY"
    engine_id_env = secrets.get("engine_id_env") or f"{module_id}_GOOGLE_SEARCH_ENGINE_ID"

    # Replace any hyphen or placeholder patterns
    api_key_env = api_key_env.replace("-", "_").replace("__MODULE_ID__", module_id)
    engine_id_env = engine_id_env.replace("-", "_").replace("__MODULE_ID__", module_id)

    if not api_key_env.startswith(f"{module_id}_"):
        api_key_env = f"{module_id}_GOOGLE_SEARCH_API_KEY"
    if not engine_id_env.startswith(f"{module_id}_"):
        engine_id_env = f"{module_id}_GOOGLE_SEARCH_ENGINE_ID"

    if secrets.get("api_key_env") != api_key_env:
        secrets["api_key_env"] = api_key_env
        actions["updated"] = True
        actions["fixes"].append("secrets.api_key_env")
    if secrets.get("engine_id_env") != engine_id_env:
        secrets["engine_id_env"] = engine_id_env
        actions["updated"] = True
        actions["fixes"].append("secrets.engine_id_env")

    data["secrets"] = secrets

    # Ensure pricing block exists
    if "pricing" not in data or not isinstance(data.get("pricing"), dict):
        data["pricing"] = {
            "unit": "api_call",
            "credits": 1,
            "scope": "per_call",
            "note": "Each Google CSE request (page fetch) costs 1 credit."
        }
        actions["updated"] = True
        actions["fixes"].append("pricing (added default)")

    # Ensure errors list exists
    if "errors" not in data or not isinstance(data.get("errors"), list) or not data.get("errors"):
        data["errors"] = [
            {
                "code": "MISSING_SECRET",
                "severity": "ERROR",
                "description": "Required environment secrets are missing for this module.",
                "remediation": "Add <ID>_GOOGLE_SEARCH_API_KEY and <ID>_GOOGLE_SEARCH_ENGINE_ID as repository secrets and pass them to env in the workflow."
            },
            {
                "code": "INVALID_INPUT",
                "severity": "ERROR",
                "description": "Work Order inputs are invalid (queries missing/too many, invalid safe value, etc.).",
                "remediation": "Validate module inputs against tenant_params.schema.json and ensure Work Order includes modules.<ID>.inputs with required fields."
            },
            {
                "code": "HTTP_RETRY_EXHAUSTED",
                "severity": "ERROR",
                "description": "Google API request failed after retries (rate limits or transient errors).",
                "remediation": "Reduce query volume, add backoff, verify quota, and retry later."
            },
            {
                "code": "OUTPUT_WRITE_FAILED",
                "severity": "ERROR",
                "description": "Module could not write output artifacts (results/report/raw).",
                "remediation": "Verify filesystem permissions and that runtime output directories exist."
            }
        ]
        actions["updated"] = True
        actions["fixes"].append("errors (added default)")

    # Ensure requirements list exists
    if "requirements" not in data or not isinstance(data.get("requirements"), list) or not data.get("requirements"):
        data["requirements"] = [
            {"type": "secret", "key": "GOOGLE_SEARCH_API_KEY", "value": "required", "note": "Provided via <ID>_GOOGLE_SEARCH_API_KEY"},
            {"type": "secret", "key": "GOOGLE_SEARCH_ENGINE_ID", "value": "required", "note": "Provided via <ID>_GOOGLE_SEARCH_ENGINE_ID"},
            {"type": "python_package", "key": "requests", "value": "required", "note": "HTTP client for Google API"},
            {"type": "output", "key": "results.jsonl", "value": "required", "note": "Normalized search results"},
            {"type": "output", "key": "report.json", "value": "required", "note": "Run report including dedupe + per-query stats"}
        ]
        actions["updated"] = True
        actions["fixes"].append("requirements (added default)")

    if actions["updated"]:
        module_yaml_path.write_text(_dump_yaml(data), encoding="utf-8")

    return actions


# ----------------------------
# Platform registries
# ----------------------------

def ensure_csv(path: Path, header: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)


def register_tenant_schema(modules_dir: Path, registry_dir: Path) -> List[str]:
    registry_dir.mkdir(parents=True, exist_ok=True)
    copied: List[str] = []

    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        module_id = m.group(1)
        tenant_schema = p / "tenant_params.schema.json"
        if tenant_schema.exists():
            out = registry_dir / f"{module_id}.schema.json"
            shutil.copyfile(tenant_schema, out)
            copied.append(module_id)

    return copied


def update_platform_tables(
    modules_dir: Path,
    module_catalog_path: Path,
    requirements_path: Path,
    prices_path: Path,
    error_reasons_path: Path
) -> Dict[str, Any]:
    """
    Upsert catalog, requirements, prices, error reasons based on module.yaml content.
    """
    ensure_csv(module_catalog_path, ["module_id","module_name","version","folder","entrypoint","description"])
    ensure_csv(requirements_path, ["module_id","requirement_type","requirement_key","requirement_value","note"])
    ensure_csv(prices_path, ["module_id","price_unit","price_credits","price_scope","note"])
    ensure_csv(error_reasons_path, ["module_id","error_code","severity","description","remediation"])

    cat_fields, cat_rows = _read_csv_rows(module_catalog_path)
    req_fields, req_rows = _read_csv_rows(requirements_path)
    price_fields, price_rows = _read_csv_rows(prices_path)
    err_fields, err_rows = _read_csv_rows(error_reasons_path)

    cat_changed = req_changed = price_changed = err_changed = 0

    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        module_id = m.group(1)
        module_yaml_path = p / "module.yaml"
        if not module_yaml_path.exists():
            continue

        data = _load_yaml(module_yaml_path)
        mod = data.get("module") or {}
        module_name = str(mod.get("name") or p.name.split("_", 1)[-1])
        version = str(mod.get("version") or "")
        entrypoint = str(mod.get("entrypoint") or "")
        description = str(mod.get("description") or "")

        # Catalog upsert
        new_cat = [{
            "module_id": module_id,
            "module_name": module_name,
            "version": version,
            "folder": p.as_posix(),
            "entrypoint": entrypoint,
            "description": description
        }]
        cat_rows, c = _upsert_rows(cat_fields or ["module_id","module_name","version","folder","entrypoint","description"], cat_rows, ["module_id"], new_cat)
        cat_changed += c

        # Pricing upsert (one row per module)
        pricing = data.get("pricing") or {}
        new_price = [{
            "module_id": module_id,
            "price_unit": str(pricing.get("unit") or ""),
            "price_credits": str(pricing.get("credits") if pricing.get("credits") is not None else ""),
            "price_scope": str(pricing.get("scope") or ""),
            "note": str(pricing.get("note") or ""),
        }]
        price_rows, c = _upsert_rows(price_fields or ["module_id","price_unit","price_credits","price_scope","note"], price_rows, ["module_id"], new_price)
        price_changed += c

        # Requirements upsert (one row per requirement)
        reqs = data.get("requirements") or []
        new_reqs: List[Dict[str, str]] = []
        for r in reqs:
            if not isinstance(r, dict):
                continue
            new_reqs.append({
                "module_id": module_id,
                "requirement_type": str(r.get("type") or ""),
                "requirement_key": str(r.get("key") or ""),
                "requirement_value": str(r.get("value") or ""),
                "note": str(r.get("note") or ""),
            })
        req_rows, c = _upsert_rows(req_fields or ["module_id","requirement_type","requirement_key","requirement_value","note"], req_rows, ["module_id","requirement_type","requirement_key"], new_reqs)
        req_changed += c

        # Error reasons upsert (one row per error code)
        errs = data.get("errors") or []
        new_errs: List[Dict[str, str]] = []
        for e in errs:
            if not isinstance(e, dict):
                continue
            new_errs.append({
                "module_id": module_id,
                "error_code": str(e.get("code") or ""),
                "severity": str(e.get("severity") or ""),
                "description": str(e.get("description") or ""),
                "remediation": str(e.get("remediation") or ""),
            })
        err_rows, c = _upsert_rows(err_fields or ["module_id","error_code","severity","description","remediation"], err_rows, ["module_id","error_code"], new_errs)
        err_changed += c

    # Persist
    _write_csv_rows(module_catalog_path, cat_fields or ["module_id","module_name","version","folder","entrypoint","description"], cat_rows)
    _write_csv_rows(prices_path, price_fields or ["module_id","price_unit","price_credits","price_scope","note"], price_rows)
    _write_csv_rows(requirements_path, req_fields or ["module_id","requirement_type","requirement_key","requirement_value","note"], req_rows)
    _write_csv_rows(error_reasons_path, err_fields or ["module_id","error_code","severity","description","remediation"], err_rows)

    return {
        "catalog_rows_changed": cat_changed,
        "price_rows_changed": price_changed,
        "requirements_rows_changed": req_changed,
        "error_reason_rows_changed": err_changed
    }


# ----------------------------
# Work Order defaults injection (optional)
# ----------------------------

def apply_defaults_from_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    if schema.get("type") != "object":
        return {}
    out: Dict[str, Any] = {}
    props = schema.get("properties") or {}
    for k, sub in props.items():
        if isinstance(sub, dict):
            if "default" in sub:
                out[k] = sub["default"]
            elif sub.get("type") == "object":
                nested = apply_defaults_from_schema(sub)
                if nested:
                    out[k] = nested
    return out


def inject_module_inputs_into_work_orders(work_orders_dir: Path, registry_dir: Path) -> int:
    updated = 0
    for path in sorted(work_orders_dir.glob("*.json")):
        try:
            wo = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        modules = wo.get("modules")
        if not isinstance(modules, dict):
            continue

        changed = False
        for module_id, mod_obj in modules.items():
            if not isinstance(mod_obj, dict):
                continue

            schema_path = registry_dir / f"{module_id}.schema.json"
            if not schema_path.exists():
                continue

            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            defaults = apply_defaults_from_schema(schema)

            if "inputs" not in mod_obj or not isinstance(mod_obj.get("inputs"), dict):
                mod_obj["inputs"] = {}
                changed = True

            inputs = mod_obj["inputs"]

            for k, v in defaults.items():
                if k not in inputs:
                    inputs[k] = v
                    changed = True
                else:
                    if isinstance(v, dict) and isinstance(inputs.get(k), dict):
                        for nk, nv in v.items():
                            if nk not in inputs[k]:
                                inputs[k][nk] = nv
                                changed = True

            modules[module_id] = mod_obj

        if changed:
            path.write_text(json.dumps(wo, ensure_ascii=False, indent=2), encoding="utf-8")
            updated += 1

    return updated


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--schema-registry-dir", default="platform/schemas/work_order_modules")
    ap.add_argument("--module-catalog-path", default="platform/modules/modules.csv")
    ap.add_argument("--requirements-path", default="platform/modules/requirements.csv")
    ap.add_argument("--prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--error-reasons-path", default="platform/errors/error_reasons.csv")
    ap.add_argument("--work-orders-dir", default="", help="Optional: directory containing work order JSON files")
    ap.add_argument("--report-path", default="runtime/maintenance_modules_report.json")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    registry_dir = Path(args.schema_registry_dir)

    if not modules_dir.exists():
        raise RuntimeError(f"Modules dir not found: {modules_dir}")

    report: Dict[str, Any] = {
        "modules_dir": str(modules_dir),
        "renamed_modules": [],
        "module_yaml_normalization": [],
        "schemas_registered": [],
        "platform_table_updates": {},
        "work_orders_updated": 0
    }

    # 1) Assign IDs + rewrite placeholders
    rename_actions = assign_ids(modules_dir)
    report["renamed_modules"] = rename_actions

    # 2) Normalize module.yaml for each assigned module
    for p in sorted(list_module_dirs(modules_dir), key=lambda x: x.name):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        module_id = m.group(1)
        if (p / "module.yaml").exists():
            report["module_yaml_normalization"].append(normalize_module_yaml(p, module_id))

    # 3) Register schemas
    report["schemas_registered"] = register_tenant_schema(modules_dir, registry_dir)

    # 4) Update platform registries
    report["platform_table_updates"] = update_platform_tables(
        modules_dir=modules_dir,
        module_catalog_path=Path(args.module_catalog_path),
        requirements_path=Path(args.requirements_path),
        prices_path=Path(args.prices_path),
        error_reasons_path=Path(args.error_reasons_path)
    )

    # 5) Work Order defaults injection (optional)
    if args.work_orders_dir:
        wo_dir = Path(args.work_orders_dir)
        if wo_dir.exists():
            report["work_orders_updated"] = inject_module_inputs_into_work_orders(wo_dir, registry_dir)

    # 6) Write report
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
