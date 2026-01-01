# Patch: maintenance helper improvements for module_prices backfill and effective date safety.
# Drop-in replacement for scripts/maintenance_modules.py (v5)
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import yaml  # PyYAML
except Exception:
    yaml = None

MODULE_ID_RE = re.compile(r"^(\d{3})_")

# Platform canonical headers (must match scripts/ci_verify.py expectations)
CATALOG_HEADER = ["module_id", "module_name", "version", "folder", "entrypoint", "description"]
REQ_HEADER = ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"]
PRICES_HEADER = ["module_id", "price_run_credits", "price_save_to_release_credits", "effective_from", "effective_to", "active", "notes"]
ERR_HEADER = ["module_id", "error_code", "severity", "description", "remediation"]

# Safety default: ensures price is "current" in any CI run date
DEFAULT_EFFECTIVE_FROM = "1970-01-01"


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or [], [dict(row) for row in r])


def _write_csv(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def _ensure_csv(path: Path, header: List[str]) -> None:
    if not path.exists():
        _write_csv(path, header, [])
        return
    got, _ = _read_csv(path)
    if got and got != header:
        raise RuntimeError(f"CSV header mismatch for {path}: expected {header} got {got}")


def _upsert_rows(header: List[str], rows: List[Dict[str, str]], key_fields: List[str], new_rows: List[Dict[str, str]]) -> int:
    idx: Dict[Tuple[str, ...], int] = {}
    for i, r in enumerate(rows):
        key = tuple((r.get(k, "") or "") for k in key_fields)
        idx[key] = i

    changed = 0
    for nr in new_rows:
        key = tuple((nr.get(k, "") or "") for k in key_fields)
        if key in idx:
            i = idx[key]
            merged = dict(rows[i])
            for k in header:
                if k in nr and nr[k] != "":
                    merged[k] = nr[k]
            if merged != rows[i]:
                rows[i] = merged
                changed += 1
        else:
            rows.append({k: nr.get(k, "") for k in header})
            changed += 1
    return changed


def _load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required for maintenance_modules.py (pip install pyyaml)")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _dump_yaml(data: Dict[str, Any]) -> str:
    if yaml is None:
        raise RuntimeError("PyYAML is required for maintenance_modules.py (pip install pyyaml)")
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


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
    s = set(used)
    while i in s:
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
    repl = {"__MODULE_ID__": module_id, "__MODULE_PREFIX__": f"{module_id}_"}
    changed = 0
    for p in module_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in {".py", ".yaml", ".yml", ".json", ".md", ".txt"}:
            if rewrite_text_file(p, repl):
                changed += 1
    return changed


def assign_ids(modules_dir: Path) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    used = parse_used_ids(modules_dir)

    for p in sorted(list_module_dirs(modules_dir), key=lambda x: x.name):
        if not is_unassigned_module_dir(p):
            continue
        new_id = next_unused_min_id(used)
        used.append(new_id)
        used.sort()

        mid = f"{new_id:03d}"
        new_name = f"{mid}_{p.name}"
        new_path = modules_dir / new_name
        if new_path.exists():
            raise RuntimeError(f"Target module dir already exists: {new_path}")

        old = p.name
        p.rename(new_path)
        rewritten = rewrite_placeholders_in_dir(new_path, mid)

        actions.append({
            "old_folder": old,
            "new_folder": new_name,
            "module_id": mid,
            "placeholders_rewritten_files_count": rewritten
        })

    return actions


def normalize_module_yaml(module_dir: Path, module_id: str) -> Dict[str, Any]:
    module_yaml_path = module_dir / "module.yaml"
    data = _load_yaml(module_yaml_path)

    actions: Dict[str, Any] = {"module_id": module_id, "updated": False, "fixes": []}

    mod = data.get("module") or {}
    if mod.get("id") != module_id:
        mod["id"] = module_id
        actions["updated"] = True
        actions["fixes"].append("module.id")

    entry = str(mod.get("entrypoint") or "")
    expected_fn = f"run_module_{module_id}"
    if expected_fn not in entry:
        entry = re.sub(r"run_module_\d{3}", expected_fn, entry)
        entry = entry.replace("run_module___MODULE_ID__", expected_fn)
        if expected_fn not in entry:
            module_name = mod.get("name") or module_dir.name.split("_", 1)[-1]
            entry = f"{module_name}:{expected_fn}"
        mod["entrypoint"] = entry
        actions["updated"] = True
        actions["fixes"].append("module.entrypoint")

    data["module"] = mod

    secrets = data.get("secrets") or {}
    api_key_env = str(secrets.get("api_key_env") or f"{module_id}_GOOGLE_SEARCH_API_KEY")
    engine_id_env = str(secrets.get("engine_id_env") or f"{module_id}_GOOGLE_SEARCH_ENGINE_ID")
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

    # Pricing normalization to platform schema
    pricing = data.get("pricing")
    if not isinstance(pricing, dict):
        pricing = {}
    pricing.setdefault("price_run_credits", 1)
    pricing.setdefault("price_save_to_release_credits", 0)
    pricing.setdefault("effective_from", DEFAULT_EFFECTIVE_FROM)
    pricing.setdefault("effective_to", "")
    pricing.setdefault("active", True)
    pricing.setdefault("notes", "Baseline per-run credit charge.")
    data["pricing"] = pricing

    if actions["updated"]:
        module_yaml_path.write_text(_dump_yaml(data), encoding="utf-8")
    return actions


def register_tenant_schema(modules_dir: Path, registry_dir: Path) -> List[str]:
    registry_dir.mkdir(parents=True, exist_ok=True)
    copied: List[str] = []
    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        mid = m.group(1)
        schema = p / "tenant_params.schema.json"
        if schema.exists():
            shutil.copyfile(schema, registry_dir / f"{mid}.schema.json")
            copied.append(mid)
    return copied


def _is_effective_now(row: Dict[str, str], today: date) -> bool:
    # Conservative parsing: empty effective_from => effective always; empty effective_to => no end.
    ef = (row.get("effective_from") or "").strip()
    et = (row.get("effective_to") or "").strip()
    active = (row.get("active") or "").strip().lower() in ("true", "1", "yes", "y")

    if not active:
        return False

    def _parse(d: str) -> date:
        y, m, dd = d.split("-")
        return date(int(y), int(m), int(dd))

    if ef:
        try:
            if _parse(ef) > today:
                return False
        except Exception:
            # if malformed, treat as not effective
            return False

    if et:
        try:
            if _parse(et) < today:
                return False
        except Exception:
            return False

    return True


def update_platform_tables(
    modules_dir: Path,
    module_catalog_path: Path,
    requirements_path: Path,
    prices_path: Path,
    error_reasons_path: Path
) -> Dict[str, Any]:
    _ensure_csv(module_catalog_path, CATALOG_HEADER)
    _ensure_csv(requirements_path, REQ_HEADER)
    _ensure_csv(prices_path, PRICES_HEADER)
    _ensure_csv(error_reasons_path, ERR_HEADER)

    _, cat_rows = _read_csv(module_catalog_path)
    _, req_rows = _read_csv(requirements_path)
    _, price_rows = _read_csv(prices_path)
    _, err_rows = _read_csv(error_reasons_path)

    today = date.today()

    cat_changed = req_changed = price_changed = err_changed = 0

    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        mid = m.group(1)
        module_yaml_path = p / "module.yaml"
        if not module_yaml_path.exists():
            continue

        data = _load_yaml(module_yaml_path)
        mod = data.get("module") or {}
        module_name = str(mod.get("name") or p.name.split("_", 1)[-1])
        version = str(mod.get("version") or "")
        entrypoint = str(mod.get("entrypoint") or "")
        description = str(mod.get("description") or "")

        cat_changed += _upsert_rows(CATALOG_HEADER, cat_rows, ["module_id"], [{
            "module_id": mid,
            "module_name": module_name,
            "version": version,
            "folder": p.as_posix(),
            "entrypoint": entrypoint,
            "description": description,
        }])

        pricing = data.get("pricing") or {}
        # Ensure that each module has at least one effective+active row "now"
        desired_row = {
            "module_id": mid,
            "price_run_credits": str(pricing.get("price_run_credits", 1)),
            "price_save_to_release_credits": str(pricing.get("price_save_to_release_credits", 0)),
            "effective_from": str(pricing.get("effective_from") or DEFAULT_EFFECTIVE_FROM),
            "effective_to": str(pricing.get("effective_to") or ""),
            "active": "true" if bool(pricing.get("active", True)) else "false",
            "notes": str(pricing.get("notes") or "Baseline per-run credit charge."),
        }

        # If a current row exists, do not duplicate; else upsert by module_id (single-row model)
        current_rows = [r for r in price_rows if (r.get("module_id") or "") == mid and _is_effective_now(r, today)]
        if current_rows:
            # still upsert by module_id to keep values updated
            price_changed += _upsert_rows(PRICES_HEADER, price_rows, ["module_id"], [desired_row])
        else:
            # Force-create current row (module_id unique)
            price_changed += _upsert_rows(PRICES_HEADER, price_rows, ["module_id"], [desired_row])

        # Requirements and errors: kept as-is (your repo may have other canonical sources).
        # This helper only guarantees presence in CSV when module.yaml provides them.
        for r in (data.get("requirements") or []):
            if isinstance(r, dict):
                req_changed += _upsert_rows(REQ_HEADER, req_rows, ["module_id", "requirement_type", "requirement_key"], [{
                    "module_id": mid,
                    "requirement_type": str(r.get("type") or ""),
                    "requirement_key": str(r.get("key") or ""),
                    "requirement_value": str(r.get("value") or ""),
                    "note": str(r.get("note") or ""),
                }])

        for e in (data.get("errors") or []):
            if isinstance(e, dict):
                err_changed += _upsert_rows(ERR_HEADER, err_rows, ["module_id", "error_code"], [{
                    "module_id": mid,
                    "error_code": str(e.get("code") or ""),
                    "severity": str(e.get("severity") or ""),
                    "description": str(e.get("description") or ""),
                    "remediation": str(e.get("remediation") or ""),
                }])

    _write_csv(module_catalog_path, CATALOG_HEADER, cat_rows)
    _write_csv(requirements_path, REQ_HEADER, req_rows)
    _write_csv(prices_path, PRICES_HEADER, price_rows)
    _write_csv(error_reasons_path, ERR_HEADER, err_rows)

    return {
        "catalog_rows_changed": cat_changed,
        "price_rows_changed": price_changed,
        "requirements_rows_changed": req_changed,
        "error_reason_rows_changed": err_changed,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--schema-registry-dir", default="platform/schemas/work_order_modules")
    ap.add_argument("--module-catalog-path", default="platform/modules/modules.csv")
    ap.add_argument("--requirements-path", default="platform/modules/requirements.csv")
    ap.add_argument("--prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--error-reasons-path", default="platform/errors/error_reasons.csv")
    ap.add_argument("--report-path", default="runtime/maintenance_modules_report.json")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    if not modules_dir.exists():
        raise RuntimeError(f"Modules dir not found: {modules_dir}")

    report: Dict[str, Any] = {
        "renamed_modules": assign_ids(modules_dir),
        "module_yaml_normalization": [],
        "schemas_registered": [],
        "platform_table_updates": {},
    }

    for p in sorted(list_module_dirs(modules_dir), key=lambda x: x.name):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        report["module_yaml_normalization"].append(normalize_module_yaml(p, m.group(1)))

    report["schemas_registered"] = register_tenant_schema(modules_dir, Path(args.schema_registry_dir))
    report["platform_table_updates"] = update_platform_tables(
        modules_dir,
        Path(args.module_catalog_path),
        Path(args.requirements_path),
        Path(args.prices_path),
        Path(args.error_reasons_path),
    )

    out = Path(args.report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
