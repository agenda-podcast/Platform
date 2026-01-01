from __future__ import annotations

import argparse
import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple


MODULE_ID_RE = re.compile(r"^(\d{3})_")


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
        if path.is_file():
            if path.suffix.lower() in {".py", ".yaml", ".yml", ".json", ".md", ".txt"}:
                if rewrite_text_file(path, replacements):
                    changed += 1
    return changed


def assign_ids(modules_dir: Path) -> List[Tuple[str, str]]:
    renamed: List[Tuple[str, str]] = []
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

        p.rename(new_path)
        rewrite_placeholders_in_dir(new_path, module_id)
        renamed.append((p.name, new_name))

    return renamed


def copy_tenant_schema_to_registry(modules_dir: Path, registry_dir: Path) -> List[str]:
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modules-dir", default="modules")
    ap.add_argument("--schema-registry-dir", default="platform/schemas/work_order_modules")
    ap.add_argument("--work-orders-dir", default="", help="Optional: directory containing work order JSON files")
    args = ap.parse_args()

    modules_dir = Path(args.modules_dir)
    registry_dir = Path(args.schema_registry_dir)

    if not modules_dir.exists():
        raise RuntimeError(f"Modules dir not found: {modules_dir}")

    renamed = assign_ids(modules_dir)
    copied = copy_tenant_schema_to_registry(modules_dir, registry_dir)

    injected = 0
    if args.work_orders_dir:
        work_orders_dir = Path(args.work_orders_dir)
        if work_orders_dir.exists():
            injected = inject_module_inputs_into_work_orders(work_orders_dir, registry_dir)

    print(json.dumps({
        "renamed_modules": renamed,
        "schemas_registered": copied,
        "work_orders_updated": injected
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
