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

# Canonical headers (match scripts/ci_verify.py expectations)
CATALOG_HEADER = ["module_id", "module_name", "version", "folder", "entrypoint", "description"]
REQ_HEADER = ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"]
PRICES_HEADER = ["module_id", "price_run_credits", "price_save_to_release_credits", "effective_from", "effective_to", "active", "notes"]
ERR_HEADER = ["module_id", "error_code", "severity", "description", "remediation"]

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


def parse_module_ids(modules_dir: Path) -> List[str]:
    ids: List[str] = []
    for p in list_module_dirs(modules_dir):
        m = MODULE_ID_RE.match(p.name)
        if m:
            ids.append(m.group(1))
    return sorted(set(ids))


def parse_used_ids(modules_dir: Path) -> List[int]:
    return sorted({int(x) for x in parse_module_ids(modules_dir)})


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
    if not module_yaml_path.exists():
        return {"module_id": module_id, "updated": False, "fixes": ["module.yaml missing"]}

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

    # Pricing normalization to platform schema (critical for orchestrate)
    pricing = data.get("pricing")
    if not isinstance(pricing, dict):
        pricing = {}
        actions["updated"] = True
        actions["fixes"].append("pricing (created)")

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
    active = (row.get("active") or "").strip().lower() in ("true", "1", "yes", "y")
    if not active:
        return False

    ef = (row.get("effective_from") or "").strip()
    et = (row.get("effective_to") or "").strip()

    def _parse(d: str) -> date:
        y, m, dd = d.split("-")
        return date(int(y), int(m), int(dd))

    if ef:
        try:
            if _parse(ef) > today:
                return False
        except Exception:
            return False

    if et:
        try:
            if _parse(et) < today:
                return False
        except Exception:
            return False

    return True


def ensure_prices_for_all_modules(prices_csv: Path, module_ids: List[str], defaults_by_id: Dict[str, Dict[str, str]] | None = None) -> Dict[str, Any]:
    defaults_by_id = defaults_by_id or {}
    _ensure_csv(prices_csv, PRICES_HEADER)
    _, rows = _read_csv(prices_csv)

    today = date.today()
    added = 0
    updated = 0

    # index by module_id
    idx = { (r.get("module_id") or "").strip(): r for r in rows if (r.get("module_id") or "").strip() }

    for mid in module_ids:
        desired = defaults_by_id.get(mid, {})
        row = idx.get(mid)

        desired_row = {
            "module_id": mid,
            "price_run_credits": desired.get("price_run_credits", "1"),
            "price_save_to_release_credits": desired.get("price_save_to_release_credits", "0"),
            "effective_from": desired.get("effective_from", DEFAULT_EFFECTIVE_FROM),
            "effective_to": desired.get("effective_to", ""),
            "active": desired.get("active", "true"),
            "notes": desired.get("notes", "Backfilled by maintenance helper."),
        }

        if row is None:
            rows.append(desired_row)
            added += 1
            idx[mid] = desired_row
            continue

        # Ensure effective+active now. If not, overwrite into the single-row model.
        if not _is_effective_now(row, today):
            for k, v in desired_row.items():
                row[k] = v
            updated += 1
        else:
            # keep as current but ensure required fields are not empty
            changed = False
            for k, v in desired_row.items():
                if (row.get(k) or "") == "" and v != "":
                    row[k] = v
                    changed = True
            if changed:
                updated += 1

    _write_csv(prices_csv, PRICES_HEADER, rows)
    return {"prices_path": str(prices_csv), "module_ids_count": len(module_ids), "rows_added": added, "rows_updated": updated}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--schema-registry-dir", default="platform/schemas/work_order_modules")
    ap.add_argument("--module-catalog-path", default="platform/modules/modules.csv")
    ap.add_argument("--requirements-path", default="platform/modules/requirements.csv")
    ap.add_argument("--prices-path", default="platform/billing/module_prices.csv")
    ap.add_argument("--error-reasons-path", default="platform/errors/error_reasons.csv")
    ap.add_argument("--billing-state-dir", default="", help="If set, ALSO writes module_prices.csv into billing-state-dir (used by orchestrate).")
    ap.add_argument("--report-path", default="runtime/maintenance_modules_report.json")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    if not modules_dir.exists():
        raise RuntimeError(f"Modules dir not found: {modules_dir}")

    report: Dict[str, Any] = {"renamed_modules": [], "module_yaml_normalization": [], "schemas_registered": [], "prices_backfill": {}}

    # 1) ID assignment + placeholder rewrite
    report["renamed_modules"] = assign_ids(modules_dir)

    # 2) Normalize module.yaml pricing (so we can use it as source-of-truth)
    defaults_by_id: Dict[str, Dict[str, str]] = {}
    for p in sorted(list_module_dirs(modules_dir), key=lambda x: x.name):
        m = MODULE_ID_RE.match(p.name)
        if not m:
            continue
        mid = m.group(1)
        report["module_yaml_normalization"].append(normalize_module_yaml(p, mid))

        # collect defaults from module.yaml pricing if present
        try:
            data = _load_yaml(p / "module.yaml")
            pricing = data.get("pricing") or {}
            defaults_by_id[mid] = {
                "price_run_credits": str(pricing.get("price_run_credits", 1)),
                "price_save_to_release_credits": str(pricing.get("price_save_to_release_credits", 0)),
                "effective_from": str(pricing.get("effective_from") or DEFAULT_EFFECTIVE_FROM),
                "effective_to": str(pricing.get("effective_to") or ""),
                "active": "true" if bool(pricing.get("active", True)) else "false",
                "notes": str(pricing.get("notes") or "Backfilled by maintenance helper."),
            }
        except Exception:
            # ignore; helper will use global defaults
            pass

    module_ids = parse_module_ids(modules_dir)

    # 3) Register schemas (unchanged)
    report["schemas_registered"] = register_tenant_schema(modules_dir, Path(args.schema_registry_dir))

    # 4) Backfill repo prices (platform path) – CI expects this header
    repo_prices = Path(args.prices_path)
    report["prices_backfill"]["repo"] = ensure_prices_for_all_modules(repo_prices, module_ids, defaults_by_id)

    # 5) Backfill billing-state prices (runtime source) – orchestrate reads from billing-state-dir
    if args.billing_state_dir:
        bs_dir = Path(args.billing_state_dir)
        bs_dir.mkdir(parents=True, exist_ok=True)
        bs_prices = bs_dir / "module_prices.csv"
        report["prices_backfill"]["billing_state"] = ensure_prices_for_all_modules(bs_prices, module_ids, defaults_by_id)

    out = Path(args.report_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
