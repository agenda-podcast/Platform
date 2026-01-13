from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml


@dataclass(frozen=True)
class StepInfo:
    step_id: str
    module_id: str
    kind: str
    enabled: bool


def _parse_workorder(repo_root: Path, tenant_id: str, work_order_id: str) -> List[StepInfo]:
    wo_path = repo_root / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
    if not wo_path.exists():
        raise AssertionError(f"workorder yaml not found: {wo_path}")
    wo = yaml.safe_load(wo_path.read_text(encoding="utf-8")) or {}

    steps: List[StepInfo] = []
    for raw in (wo.get("steps") or []):
        if not isinstance(raw, dict):
            continue
        step_id = str(raw.get("step_id") or "").strip()
        module_id = str(raw.get("module_id") or "").strip()
        kind = str(raw.get("kind") or "").strip()
        enabled = bool(raw.get("enabled", True))
        if not step_id or not module_id:
            continue
        steps.append(StepInfo(step_id=step_id, module_id=module_id, kind=kind, enabled=enabled))
    return steps


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def _meta(row: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return json.loads(str(row.get("metadata_json") or "{}"))
    except Exception:
        return {}


def _dropbox_verification_status(payload: dict) -> str:
    # Supports both legacy report.json and delivery_receipt.json payloads
    if not isinstance(payload, dict):
        return ""
    if "verification_status" in payload:
        return str(payload.get("verification_status") or "")
    verification = payload.get("verification") or {}
    if isinstance(verification, dict) and "status" in verification:
        return str(verification.get("status") or "")
    if "status" in payload:
        return str(payload.get("status") or "")
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    steps = [s for s in _parse_workorder(repo_root, args.tenant_id, args.work_order_id) if s.enabled]
    pd_steps = [s for s in steps if s.kind in ("packaging", "delivery")]

    ti_path = Path(args.billing_state_dir) / "transaction_items.csv"
    assert ti_path.exists(), f"transaction_items.csv not found: {ti_path}"

    rows = _read_csv(ti_path)

    # Filter to SPEND __run__ rows for packaging/delivery steps.
    spend_rows: List[Dict[str, Any]] = []
    for r in rows:
        if str(r.get("tenant_id") or "").strip() != args.tenant_id:
            continue
        if str(r.get("work_order_id") or "").strip() != args.work_order_id:
            continue
        if str(r.get("type") or "").strip().upper() != "SPEND":
            continue
        if str(r.get("deliverable_id") or "").strip() != "__run__":
            continue
        spend_rows.append(r)

    # Assert no duplicate SPEND rows per (module_id, step_id) for packaging/delivery.
    key_counts: Dict[Tuple[str, str], int] = {}
    idem_keys: List[str] = []

    pd_key_set = {(s.module_id, s.step_id) for s in pd_steps}

    for r in spend_rows:
        module_id = str(r.get("module_id") or "").strip()
        step_id = str(r.get("step_id") or "").strip()
        if (module_id, step_id) not in pd_key_set:
            continue
        k = (module_id, step_id)
        key_counts[k] = key_counts.get(k, 0) + 1
        idem = str(_meta(r).get("idempotency_key") or "").strip()
        assert idem, f"missing metadata_json.idempotency_key for SPEND row module={module_id} step={step_id}"
        idem_keys.append(idem)

    for s in pd_steps:
        k = (s.module_id, s.step_id)
        assert key_counts.get(k, 0) == 1, f"duplicate SPEND rows for module={s.module_id} step={s.step_id} (expected 1, got {key_counts.get(k, 0)})"

    # Ensure idempotency keys are unique across packaging/delivery __run__ spends.
    assert len(idem_keys) == len(set(idem_keys)), "duplicate metadata_json.idempotency_key across packaging/delivery SPEND rows"

    # Delivery idempotency (Dropbox): second run should skip upload if remote exists.
    # We assert the latest COMPLETED deliver_dropbox run report has verification_status='already_exists_verified'
    # when there are 2+ completed runs for that delivery step.
    mr_path = Path(args.billing_state_dir) / "module_runs_log.csv"
    if mr_path.exists():
        mr = _read_csv(mr_path)
        # Find delivery step_id for deliver_dropbox.
        dd_steps = [s for s in pd_steps if s.module_id == "deliver_dropbox"]
        for dd in dd_steps:
            completed = []
            for row in mr:
                if str(row.get("tenant_id") or "").strip() != args.tenant_id:
                    continue
                if str(row.get("work_order_id") or "").strip() != args.work_order_id:
                    continue
                if str(row.get("module_id") or "").strip() != "deliver_dropbox":
                    continue
                if str(row.get("status") or "").strip().upper() != "COMPLETED":
                    continue
                try:
                    m = json.loads(str(row.get("metadata_json") or "{}"))
                except Exception:
                    m = {}
                if str(m.get("step_id") or "").strip() != dd.step_id:
                    continue
                completed.append(row)

            if len(completed) >= 2:
                completed_sorted = sorted(completed, key=lambda r: str(r.get("ended_at") or r.get("created_at") or "").strip())
                latest = completed_sorted[-1]
                try:
                    m = json.loads(str(latest.get("metadata_json") or "{}"))
                except Exception:
                    m = {}
                od = str(m.get("outputs_dir") or latest.get("output_ref") or "").strip()
                assert od, "deliver_dropbox run missing outputs_dir"
                rep = Path(od) / "delivery_receipt.json"
                assert rep.exists(), f"deliver_dropbox delivery_receipt.json missing: {rep}"
                rj = json.loads(rep.read_text(encoding="utf-8"))
                vs = str(rj.get("verification_status") or "").strip()
                assert vs == "already_exists_verified", f"deliver_dropbox second-run verification_status expected already_exists_verified, got {vs}"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
