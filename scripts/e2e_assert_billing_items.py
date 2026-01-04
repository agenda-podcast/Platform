#!/usr/bin/env python3
"""E2E assertions for billing-state ledger itemization.

This test verifies that when a module is configured with
purchase_release_artifacts=true, the orchestrator records TWO line-items:

  - RUN
  - SAVE_ARTIFACTS

and that refunds also itemize both lines with non-empty notes.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure repo root is importable even when executed as a file (sys.path[0] would be ./scripts).
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from platform.utils.csvio import read_csv


def _parse_iso(ts: str) -> datetime:
    t = (ts or "").strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    return datetime.fromisoformat(t)


def _latest(rows, *, predicate):
    candidates = [r for r in rows if predicate(r)]
    if not candidates:
        return None
    candidates.sort(key=lambda r: _parse_iso(str(r.get("created_at", ""))), reverse=True)
    return candidates[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--tenant-id", default="nxlkGI")
    ap.add_argument("--work-order-id", default="UbjkpxZO")
    ap.add_argument("--module-id", default="wxz")
    ap.add_argument("--expected-reason-code", default="RCNCl7")
    args = ap.parse_args()

    bdir = Path(args.billing_state_dir)
    txs = read_csv(bdir / "transactions.csv")
    items = read_csv(bdir / "transaction_items.csv")

    tenant_id = args.tenant_id
    work_order_id = args.work_order_id
    module_id = args.module_id

    spend = _latest(
        txs,
        predicate=lambda r: r.get("tenant_id") == tenant_id
        and r.get("work_order_id") == work_order_id
        and str(r.get("type", "")).upper() == "SPEND",
    )
    if not spend:
        raise SystemExit(f"E2E assert failed: no SPEND transaction for tenant={tenant_id} work_order_id={work_order_id}")

    spend_id = spend.get("transaction_id")
    spend_items = [i for i in items if i.get("transaction_id") == spend_id and i.get("module_id") == module_id]
    spend_features = {str(i.get("feature", "")).upper() for i in spend_items}

    missing = {"RUN", "SAVE_ARTIFACTS"} - spend_features
    if missing:
        raise SystemExit(f"E2E assert failed: SPEND transaction_item features missing {sorted(missing)}; got={sorted(spend_features)}")

    for i in spend_items:
        feat = str(i.get("feature", "")).upper()
        if feat in {"RUN", "SAVE_ARTIFACTS"}:
            note = str(i.get("note", "")).strip()
            if not note:
                raise SystemExit(f"E2E assert failed: SPEND item note is empty for feature={feat}")
            if module_id not in note:
                raise SystemExit(f"E2E assert failed: SPEND item note does not reference module_id={module_id}: {note}")

    refund = _latest(
        txs,
        predicate=lambda r: r.get("tenant_id") == tenant_id
        and r.get("work_order_id") == work_order_id
        and str(r.get("type", "")).upper() == "REFUND",
    )
    if not refund:
        raise SystemExit(f"E2E assert failed: no REFUND transaction for tenant={tenant_id} work_order_id={work_order_id}")

    refund_id = refund.get("transaction_id")
    refund_items = [i for i in items if i.get("transaction_id") == refund_id and i.get("module_id") == module_id]
    refund_features = {str(i.get("feature", "")).upper() for i in refund_items}
    missing_r = {"RUN", "SAVE_ARTIFACTS"} - refund_features
    if missing_r:
        raise SystemExit(f"E2E assert failed: REFUND transaction_item features missing {sorted(missing_r)}; got={sorted(refund_features)}")

    expected_rc = args.expected_reason_code
    for i in refund_items:
        feat = str(i.get("feature", "")).upper()
        if feat in {"RUN", "SAVE_ARTIFACTS"}:
            note = str(i.get("note", "")).strip()
            if not note:
                raise SystemExit(f"E2E assert failed: REFUND item note is empty for feature={feat}")
            if module_id not in note:
                raise SystemExit(f"E2E assert failed: REFUND item note does not reference module_id={module_id}: {note}")
            if expected_rc and expected_rc not in note:
                raise SystemExit(
                    f"E2E assert failed: REFUND item note does not reference expected reason_code={expected_rc}: {note}"
                )

    print("[E2E] Billing itemization assertions: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
