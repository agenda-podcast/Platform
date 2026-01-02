from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Set

from ..utils.csvio import read_csv


def load_dependency_index(path: Path) -> Dict[str, List[str]]:
    rows = read_csv(path)
    out: Dict[str, List[str]] = {}
    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        if not mid:
            continue
        deps_raw = str(r.get("depends_on_module_ids", "[]")).strip() or "[]"
        try:
            deps = json.loads(deps_raw)
        except Exception:
            deps = []
        out[mid] = [str(x) for x in deps]
    return out


def topo_sort(modules: List[str], deps: Dict[str, List[str]]) -> List[str]:
    """Topo sort restricted to `modules`. Raises ValueError on missing deps or cycles."""
    wanted = list(dict.fromkeys(modules))
    wanted_set = set(wanted)

    # Ensure all dependencies are present in the requested set
    missing: Set[str] = set()
    for m in wanted:
        for d in deps.get(m, []):
            if d and d not in wanted_set:
                missing.add(d)
    if missing:
        raise ValueError(f"Work order missing required dependency modules: {sorted(missing)}")

    temporary: Set[str] = set()
    permanent: Set[str] = set()
    result: List[str] = []

    def visit(n: str) -> None:
        if n in permanent:
            return
        if n in temporary:
            raise ValueError(f"Dependency cycle detected at module {n}")
        temporary.add(n)
        for d in deps.get(n, []):
            if d in wanted_set:
                visit(d)
        temporary.remove(n)
        permanent.add(n)
        result.append(n)

    for m in wanted:
        if m not in permanent:
            visit(m)

    return result
