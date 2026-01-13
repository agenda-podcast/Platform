from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def _meta(meta_json: str) -> Dict[str, Any]:
    try:
        return json.loads(meta_json or "{}")
    except Exception:
        return {}


def _parse_threshold_bytes(repo_root: Path) -> int:
    run_py = repo_root / "modules" / "deliver_email" / "src" / "run.py"
    txt = run_py.read_text(encoding="utf-8")
    m = re.search(r"^MAX_PACKAGE_BYTES\s*=\s*(\d+)\s*$", txt, flags=re.MULTILINE)
    if not m:
        raise AssertionError("could not parse MAX_PACKAGE_BYTES from deliver_email run.py")
    return int(m.group(1))


def _parse_workorder(repo_root: Path, tenant_id: str, work_order_id: str) -> Dict[str, Any]:
    wo_path = repo_root / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
    if not wo_path.exists():
        raise AssertionError(f"workorder yaml not found: {wo_path}")
    return yaml.safe_load(wo_path.read_text(encoding="utf-8")) or {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    threshold = _parse_threshold_bytes(repo_root)
    wo = _parse_workorder(repo_root, args.tenant_id, args.work_order_id)

    enabled_steps = [s for s in (wo.get("steps") or []) if isinstance(s, dict) and bool(s.get("enabled", True))]

    packaging_steps = [s for s in enabled_steps if str(s.get("kind") or "").strip() == "packaging"]
    delivery_steps = [s for s in enabled_steps if str(s.get("kind") or "").strip() == "delivery" and str(s.get("module_id") or "").strip() == "deliver_email"]

    assert packaging_steps, "email threshold workorder must include a packaging step"
    assert delivery_steps, "email threshold workorder must include a deliver_email delivery step"

    pkg_step_id = str(packaging_steps[0].get("step_id") or "").strip()
    email_step_id = str(delivery_steps[0].get("step_id") or "").strip()

    mr_path = Path(args.billing_state_dir) / "module_runs_log.csv"
    assert mr_path.exists(), f"module_runs_log.csv not found: {mr_path}"
    mr = _read_csv(mr_path)

    pkg_runs = []
    email_runs = []
    for row in mr:
        if str(row.get("tenant_id") or "").strip() != args.tenant_id:
            continue
        if str(row.get("work_order_id") or "").strip() != args.work_order_id:
            continue

        meta = _meta(str(row.get("metadata_json") or ""))
        sid = str(meta.get("step_id") or "").strip()
        mid = str(row.get("module_id") or "").strip()
        if sid == pkg_step_id and mid == "package_std" and str(row.get("status") or "").strip().upper() == "COMPLETED":
            pkg_runs.append(row)
        if sid == email_step_id and mid == "deliver_email":
            email_runs.append(row)

    assert pkg_runs, "no COMPLETED package_std run found for email threshold scenario"
    # packaging outputs dir
    pkg_latest = sorted(pkg_runs, key=lambda r: str(r.get("ended_at") or r.get("created_at") or "").strip())[-1]
    pkg_meta = _meta(str(pkg_latest.get("metadata_json") or ""))
    pkg_out_dir = str(pkg_meta.get("outputs_dir") or pkg_latest.get("output_ref") or "").strip()
    assert pkg_out_dir, "package_std run missing outputs_dir/output_ref"
    pkg_zip = Path(pkg_out_dir) / "package.zip"
    assert pkg_zip.exists(), f"package.zip missing: {pkg_zip}"
    pkg_size = int(pkg_zip.stat().st_size)
    assert pkg_size >= threshold, f"package.zip must be >= email threshold ({threshold} bytes), got {pkg_size}"

    # deliver_email must FAIL with reason_slug package_too_large_for_email
    failed = [r for r in email_runs if str(r.get("status") or "").strip().upper() == "FAILED"]
    assert failed, "deliver_email did not FAIL in email threshold scenario"
    latest_failed = sorted(failed, key=lambda r: str(r.get("ended_at") or r.get("created_at") or "").strip())[-1]
    em_meta = _meta(str(latest_failed.get("metadata_json") or ""))
    em_out_dir = str(em_meta.get("outputs_dir") or latest_failed.get("output_ref") or "").strip()
    assert em_out_dir, "deliver_email failed run missing outputs_dir"
    rep = Path(em_out_dir) / "report.json"
    assert rep.exists(), f"deliver_email report.json missing: {rep}"
    rj = json.loads(rep.read_text(encoding="utf-8"))
    reason_slug = str(rj.get("reason_slug") or "").strip()
    assert reason_slug == "package_too_large_for_email", f"deliver_email reason_slug expected package_too_large_for_email, got {reason_slug}"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
