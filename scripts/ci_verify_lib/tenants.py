from __future__ import annotations

from pathlib import Path

from platform.common.id_policy import validate_id
from platform.utils.csvio import read_csv, require_headers

from . import core


def _validate_tenants(repo_root: Path) -> None:
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        core._fail("tenants/ directory missing")

    # tenant folders must be valid IDs
    for d in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not d.is_dir():
            continue
        tid = d.name.strip()
        validate_id("tenant_id", tid, "tenant_id")

    # spot-check platform tenant workorders path exists if tenant 000000 exists
    platform_tenant = tenants_dir / "000000"
    if platform_tenant.exists():
        wo_dir = platform_tenant / "workorders"
        if not wo_dir.exists():
            core._fail("Platform tenant 000000 exists but tenants/000000/workorders missing")

    # maintenance-state/workorders_index.csv header and ID formats
    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    require_headers(idx, ["tenant_id","work_order_id","path","enabled"])
    # allow additional columns; maintenance may add metadata fields

    rows = read_csv(idx)
    seen = set()
    for r in rows:
        wid = str(r.get("work_order_id", "")).strip()
        if not wid:
            continue
        validate_id("work_order_id", wid, "workorders_index.work_order_id")
        if wid in seen:
            core._fail(f"Duplicate work_order_id in workorders_index.csv: {wid}")
        seen.add(wid)

        tid = str(r.get("tenant_id", "")).strip()
        if tid:
            validate_id("tenant_id", tid, "workorders_index.tenant_id")

    core._ok("Tenants: folder IDs + workorders_index OK")
