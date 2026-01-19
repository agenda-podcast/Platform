from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

RE_ENDS_LETTER = re.compile(r"[A-Za-z]$")
RE_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")

SUFFIX = "t"  # deterministic suffix


def normalize_id(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    s = value.strip()
    if not s:
        return s
    if not RE_SAFE_ID.match(s):
        return value
    if RE_ENDS_LETTER.search(s):
        return s
    return s + SUFFIX


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def dump_yaml(path: Path, obj: Any) -> None:
    path.write_text(yaml.safe_dump(obj, sort_keys=False, allow_unicode=False), encoding="utf-8")


def rename_tenant_dir(root: Path, old: str, new: str) -> bool:
    tenants_dir = root / "tenants"
    old_dir = tenants_dir / old
    new_dir = tenants_dir / new
    if not old_dir.exists():
        return False
    if new_dir.exists():
        raise RuntimeError(f"Target tenant dir already exists: {new_dir}")
    old_dir.rename(new_dir)
    return True


def transform_tenant_yaml(path: Path, tenant_rename: Dict[str, str]) -> bool:
    if not path.exists():
        return False
    obj = load_yaml(path)
    if not isinstance(obj, dict):
        return False

    changed = False
    tid = str(obj.get("tenant_id", "") or "")
    if tid in tenant_rename:
        obj["tenant_id"] = tenant_rename[tid]
        changed = True

    rel = obj.get("related_tenant_ids")
    if isinstance(rel, list):
        new_rel: List[Any] = []
        for v in rel:
            if isinstance(v, str) and v in tenant_rename:
                new_rel.append(tenant_rename[v])
                changed = True
            else:
                new_rel.append(v)
        obj["related_tenant_ids"] = new_rel

    if changed:
        dump_yaml(path, obj)
    return changed


def transform_workorder_yaml(path: Path, tenant_rename: Dict[str, str]) -> Tuple[bool, str, str]:
    obj = load_yaml(path)
    if not isinstance(obj, dict):
        return False, "", ""

    changed = False

    old_wo = str(obj.get("work_order_id", "") or "")
    new_wo = ""
    if old_wo:
        new_wo = str(normalize_id(old_wo))
        if new_wo != old_wo:
            obj["work_order_id"] = new_wo
            changed = True

    tid = str(obj.get("tenant_id", "") or "")
    if tid in tenant_rename:
        obj["tenant_id"] = tenant_rename[tid]
        changed = True

    steps = obj.get("steps")
    step_map: Dict[str, str] = {}
    if isinstance(steps, list):
        for st in steps:
            if not isinstance(st, dict):
                continue
            sid = st.get("step_id")
            if isinstance(sid, str) and sid:
                nsid = str(normalize_id(sid))
                if nsid != sid:
                    st["step_id"] = nsid
                    step_map[sid] = nsid
                    changed = True

            for k, v in list(st.items()):
                if not isinstance(k, str):
                    continue
                if k.endswith("_id") and isinstance(v, str) and v:
                    if k == "module_id":
                        continue
                    nv = str(normalize_id(v))
                    if nv != v:
                        st[k] = nv
                        changed = True

    def walk(node: Any) -> Any:
        nonlocal changed
        if isinstance(node, dict):
            out: Dict[Any, Any] = {}
            for k, v in node.items():
                if k == "from_step" and isinstance(v, str) and v in step_map:
                    out[k] = step_map[v]
                    changed = True
                    continue
                if isinstance(k, str) and k.endswith("_id") and isinstance(v, str) and v:
                    if k == "module_id":
                        out[k] = v
                    else:
                        nv = str(normalize_id(v))
                        if nv != v:
                            changed = True
                        out[k] = nv
                    continue
                if isinstance(v, str) and v in step_map:
                    out[k] = step_map[v]
                    changed = True
                    continue
                if isinstance(v, str) and v in tenant_rename:
                    out[k] = tenant_rename[v]
                    changed = True
                    continue
                out[k] = walk(v)
            return out
        if isinstance(node, list):
            return [walk(x) for x in node]
        if isinstance(node, str) and node in step_map:
            changed = True
            return step_map[node]
        if isinstance(node, str) and node in tenant_rename:
            changed = True
            return tenant_rename[node]
        return node

    obj = walk(obj)

    if changed:
        dump_yaml(path, obj)

    final_wo = str(obj.get("work_order_id", "") or "")
    return changed, old_wo, final_wo


def transform_workorders(root: Path, tenant_rename: Dict[str, str]) -> Dict[str, str]:
    wo_id_map: Dict[str, str] = {}
    tenants_dir = root / "tenants"
    if not tenants_dir.exists():
        return wo_id_map

    for yml in tenants_dir.glob("*/workorders/*.yml"):
        changed, old_wo, new_wo = transform_workorder_yaml(yml, tenant_rename)
        if old_wo and new_wo and old_wo != new_wo:
            wo_id_map[old_wo] = new_wo
            new_path = yml.with_name(f"{new_wo}.yml")
            if new_path != yml:
                yml.rename(new_path)

    return wo_id_map


def rewrite_csv_ids(path: Path, col_names: List[str], tenant_rename: Dict[str, str], wo_id_map: Dict[str, str]) -> bool:
    if not path.exists():
        return False

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows: List[Dict[str, str]] = []
        for r in reader:
            rows.append({k: (r.get(k, "") or "") for k in header})

    changed = False
    for r in rows:
        for c in col_names:
            if c not in r:
                continue
            v = r[c]
            if c == "tenant_id" and v in tenant_rename:
                r[c] = tenant_rename[v]
                changed = True
                continue
            if c == "related_tenant_id" and v in tenant_rename:
                r[c] = tenant_rename[v]
                changed = True
                continue
            if c == "work_order_id" and v in wo_id_map:
                r[c] = wo_id_map[v]
                changed = True
                continue
            nv = str(normalize_id(v))
            if nv != v:
                r[c] = nv
                changed = True

    if changed:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    return changed


def rewrite_repo_text(root: Path, tenant_rename: Dict[str, str], wo_id_map: Dict[str, str]) -> int:
    count = 0
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if "/.git/" in str(path):
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".zip"}:
            continue
        try:
            txt = path.read_text(encoding="utf-8")
        except Exception:
            continue

        new = txt
        for a, b in tenant_rename.items():
            new = new.replace(a, b)
            new = new.replace(f"tenants/{a}/", f"tenants/{b}/")
            new = new.replace(f"tenants/{a}", f"tenants/{b}")
        for old_wo, new_wo in wo_id_map.items():
            new = new.replace(old_wo, new_wo)
            new = new.replace(f"/{old_wo}.yml", f"/{new_wo}.yml")

        if new != txt:
            path.write_text(new, encoding="utf-8")
            count += 1
    return count


def main() -> int:
    root = Path(__file__).resolve().parents[1]

    tenant_rename = {"00000t": "00000t"}

    renamed = rename_tenant_dir(root, "00000t", "00000t")

    tenant_yml = root / "tenants" / "00000t" / "tenant.yml"
    transform_tenant_yaml(tenant_yml, tenant_rename)

    wo_id_map = transform_workorders(root, tenant_rename)

    csv_jobs = [
        (root / "maintenance-state" / "workorders_index.csv", ["tenant_id", "work_order_id"]),
        (root / "maintenance-state" / "tenant_relationships.csv", ["tenant_id", "related_tenant_id"]),
        (root / "billing-state-seed" / "tenants_credits.csv", ["tenant_id"]),
        (root / "releases" / "billing-state-v1" / "tenants_credits.csv", ["tenant_id"]),
        (root / ".billing-state-ci" / "tenants_credits.csv", ["tenant_id"]),
        (root / ".billing-state-test" / "tenants_credits.csv", ["tenant_id"]),
    ]

    for p, cols in csv_jobs:
        rewrite_csv_ids(p, cols, tenant_rename, wo_id_map)

    touched = rewrite_repo_text(root, tenant_rename, wo_id_map)

    print("RENAMED_TENANT_DIR=", renamed)
    print("WORK_ORDER_ID_MAP=", wo_id_map)
    print("TEXT_FILES_TOUCHED=", touched)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
