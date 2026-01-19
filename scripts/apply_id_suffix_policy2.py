from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

RE_ENDS_LETTER = re.compile(r"[A-Za-z]$")
RE_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")

SUFFIX = "t"


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
    old_dir = root / "tenants" / old
    new_dir = root / "tenants" / new
    if not old_dir.exists():
        return False
    if new_dir.exists():
        raise RuntimeError(f"Target tenant dir already exists: {new_dir}")
    old_dir.rename(new_dir)
    return True


def transform_tenant_yaml(path: Path, tenant_rename: Dict[str, str]) -> bool:
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
        for x in rel:
            if isinstance(x, str) and x in tenant_rename:
                new_rel.append(tenant_rename[x])
                changed = True
            else:
                new_rel.append(x)
        obj["related_tenant_ids"] = new_rel
    if changed:
        dump_yaml(path, obj)
    return changed


def walk_replace_ids(node: Any, step_map: Dict[str, str], tenant_rename: Dict[str, str]) -> Any:
    if isinstance(node, dict):
        out: Dict[Any, Any] = {}
        for k, v in node.items():
            if k == "from_step" and isinstance(v, str) and v in step_map:
                out[k] = step_map[v]
                continue
            if isinstance(k, str) and k.endswith("_id") and isinstance(v, str) and v:
                if k == "module_id":
                    out[k] = v
                    continue
                if k == "tenant_id" and v in tenant_rename:
                    out[k] = tenant_rename[v]
                    continue
                out[k] = normalize_id(v)
                continue
            out[k] = walk_replace_ids(v, step_map, tenant_rename)
        return out
    if isinstance(node, list):
        return [walk_replace_ids(x, step_map, tenant_rename) for x in node]
    if isinstance(node, str) and node in step_map:
        return step_map[node]
    if isinstance(node, str) and node in tenant_rename:
        return tenant_rename[node]
    return node


def transform_workorder_yaml(path: Path, tenant_rename: Dict[str, str]) -> Tuple[bool, str, str]:
    obj = load_yaml(path)
    if not isinstance(obj, dict):
        return False, "", ""

    changed = False
    old_wo = str(obj.get("work_order_id", "") or "")
    if old_wo:
        new_wo = normalize_id(old_wo)
        if new_wo != old_wo:
            obj["work_order_id"] = new_wo
            changed = True
    else:
        new_wo = ""

    tid = str(obj.get("tenant_id", "") or "")
    if tid in tenant_rename:
        obj["tenant_id"] = tenant_rename[tid]
        changed = True

    step_map: Dict[str, str] = {}
    steps = obj.get("steps")
    if isinstance(steps, list):
        for st in steps:
            if not isinstance(st, dict):
                continue
            sid = st.get("step_id")
            if isinstance(sid, str) and sid:
                nsid = normalize_id(sid)
                if nsid != sid:
                    st["step_id"] = nsid
                    step_map[sid] = nsid
                    changed = True
            for k, v in list(st.items()):
                if isinstance(k, str) and k.endswith("_id") and isinstance(v, str) and v:
                    if k == "module_id":
                        continue
                    nv = normalize_id(v)
                    if nv != v:
                        st[k] = nv
                        changed = True

    obj2 = walk_replace_ids(obj, step_map, tenant_rename)
    if obj2 != obj:
        obj = obj2
        changed = True

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


def rewrite_csv(path: Path, tenant_rename: Dict[str, str], wo_id_map: Dict[str, str]) -> bool:
    if not path.exists():
        return False
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = [r for r in reader]

    changed = False
    for r in rows:
        for k in list(r.keys()):
            v = r.get(k, "") or ""
            if k == "tenant_id" and v in tenant_rename:
                r[k] = tenant_rename[v]
                changed = True
                continue
            if k == "related_tenant_id" and v in tenant_rename:
                r[k] = tenant_rename[v]
                changed = True
                continue
            if k == "work_order_id" and v in wo_id_map:
                r[k] = wo_id_map[v]
                changed = True
                continue
            if k.endswith("_id") and isinstance(v, str) and v:
                if k == "module_id":
                    continue
                nv = normalize_id(v)
                if nv != v:
                    r[k] = nv
                    changed = True

    if changed:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
    return changed


def rewrite_text_refs(root: Path, tenant_rename: Dict[str, str], wo_id_map: Dict[str, str]) -> int:
    count = 0
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        if "/.git/" in str(path):
            continue
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".zip", ".pdf"}:
            continue
        try:
            txt = path.read_text(encoding="utf-8")
        except Exception:
            continue
        new = txt
        for a, b in tenant_rename.items():
            new = new.replace(a, b)
            new = new.replace(f"tenants/{a}/", f"tenants/{b}/")
        for a, b in wo_id_map.items():
            new = new.replace(a, b)
        if new != txt:
            path.write_text(new, encoding="utf-8")
            count += 1
    return count


def main() -> int:
    root = Path(__file__).resolve().parents[1]

    tenant_rename = {"00000t": "00000t"}

    renamed = rename_tenant_dir(root, "00000t", "00000t")

    tenant_yml = root / "tenants" / "00000t" / "tenant.yml"
    if tenant_yml.exists():
        transform_tenant_yaml(tenant_yml, tenant_rename)

    wo_id_map = transform_workorders(root, tenant_rename)

    for p in root.rglob("*.csv"):
        rewrite_csv(p, tenant_rename, wo_id_map)

    rewritten = rewrite_text_refs(root, tenant_rename, wo_id_map)

    print("RENAMED_TENANT_DIR=", renamed)
    print("WORK_ORDER_ID_MAP=", wo_id_map)
    print("TEXT_FILES_REWRITTEN=", rewritten)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
