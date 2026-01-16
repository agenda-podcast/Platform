from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class ResolvedWorkorder:
    tenant_id: str
    work_order_id: str
    path: str


def _read_workorders_index(repo_root: Path) -> List[Dict[str, str]]:
    idx = Path(repo_root) / "maintenance-state" / "workorders_index.csv"
    if not idx.exists():
        raise FileNotFoundError(f"Missing workorders index (run Maintenance): {idx}")

    with idx.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if r.fieldnames is None:
            raise ValueError(f"workorders_index.csv missing header: {idx}")
        out: List[Dict[str, str]] = []
        for row in r:
            out.append({k: (v or "") for k, v in row.items()})
        return out


def resolve_workorder_by_id(repo_root: Path, work_order_id: str) -> ResolvedWorkorder:
    """Resolve a global work_order_id to a single workorder path.

    Source of truth: maintenance-state/workorders_index.csv.

    This resolver intentionally does not check the enabled flag. Verification runners
    can validate or execute disabled workorders deterministically.
    """

    wid = str(work_order_id or "").strip()
    if not wid:
        raise ValueError("work_order_id is empty")

    rows = _read_workorders_index(Path(repo_root))
    hits = [r for r in rows if str(r.get("work_order_id", "") or "").strip() == wid]

    if not hits:
        raise KeyError(f"work_order_id not found in workorders_index.csv: {wid}")

    if len(hits) != 1:
        paths = sorted({str(h.get("path", "") or "").strip() for h in hits if str(h.get("path", "") or "").strip()})
        raise ValueError(f"work_order_id is not unique in workorders_index.csv: {wid}; paths={paths}")

    tid = str(hits[0].get("tenant_id", "") or "").strip()
    rel = str(hits[0].get("path", "") or "").strip()

    if not tid:
        raise ValueError(f"tenant_id missing for work_order_id={wid}")
    if not rel:
        raise ValueError(f"path missing for work_order_id={wid}")

    return ResolvedWorkorder(tenant_id=tid, work_order_id=wid, path=rel)


def write_single_workorder_index(repo_root: Path, resolved: ResolvedWorkorder, out_csv_path: Path) -> None:
    """Write a minimal workorders_index.csv containing only the selected workorder.

    The orchestrator reads workorders_index.csv and only queues rows where enabled=true.
    """

    out_csv_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "tenant_id": resolved.tenant_id,
        "work_order_id": resolved.work_order_id,
        "enabled": "true",
        "schedule_cron": "",
        "title": "",
        "notes": "",
        "path": resolved.path,
    }

    headers = ["tenant_id", "work_order_id", "enabled", "schedule_cron", "title", "notes", "path"]
    with out_csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerow(row)
