from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def _meta(meta_json: str) -> Dict[str, Any]:
    try:
        return json.loads(meta_json or "{}")
    except Exception:
        return {}


def _reason_code_for_slug(repo_root: Path, slug: str) -> str:
    cat_path = repo_root / "maintenance-state" / "reason_catalog.csv"
    rows = _read_csv(cat_path)
    for r in rows:
        if str(r.get("scope") or "").strip() != "GLOBAL":
            continue
        if str(r.get("reason_slug") or "").strip() == slug:
            return str(r.get("reason_code") or "").strip()
    raise AssertionError(f"GLOBAL reason_slug not found in reason_catalog.csv: {slug}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    expected_rc = _reason_code_for_slug(repo_root, "secrets_missing")

    wo_log_path = Path(args.billing_state_dir) / "workorders_log.csv"
    tx_path = Path(args.billing_state_dir) / "transactions.csv"

    assert wo_log_path.exists(), f"workorders_log.csv not found: {wo_log_path}"
    assert tx_path.exists(), f"transactions.csv not found: {tx_path}"

    wo_rows = _read_csv(wo_log_path)
    tx_rows = _read_csv(tx_path)

    matches = []
    for r in wo_rows:
        if str(r.get("tenant_id") or "").strip() != args.tenant_id:
            continue
        if str(r.get("work_order_id") or "").strip() != args.work_order_id:
            continue
        matches.append(r)

    assert matches, "no workorders_log row found for secret preflight scenario"
    latest = sorted(matches, key=lambda r: str(r.get("ended_at") or r.get("started_at") or r.get("created_at") or "").strip())[-1]
    status = str(latest.get("status") or "").strip().upper()
    assert status == "FAILED", f"expected workorder status FAILED, got {status}"

    meta = _meta(str(latest.get("metadata_json") or ""))
    rc = str(meta.get("reason_code") or "").strip()
    assert rc == expected_rc, f"expected reason_code {expected_rc} for secrets_missing, got {rc}"

    missing = meta.get("missing_secrets")
    assert isinstance(missing, list) and missing, "expected missing_secrets list in workorders_log metadata_json"

    # Ensure preflight failed before spend transaction was recorded.
    spends = []
    for t in tx_rows:
        if str(t.get("tenant_id") or "").strip() != args.tenant_id:
            continue
        if str(t.get("work_order_id") or "").strip() != args.work_order_id:
            continue
        if str(t.get("type") or "").strip().upper() == "SPEND":
            spends.append(t)

    assert not spends, "SPEND transaction should not exist when preflight secret gate fails"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
