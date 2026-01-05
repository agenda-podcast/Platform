from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def _latest_run_dir(base: Path) -> Optional[Path]:
    if not base.exists() or not base.is_dir():
        return None
    # Step layout: <base>/<module_run_id>/...
    dirs = [p for p in base.iterdir() if p.is_dir()]
    if not dirs:
        return None
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs[0]


def _assert_file_exists(p: Path, min_bytes: int = 1) -> None:
    if not p.exists() or not p.is_file():
        raise SystemExit(f"[E2E][FAIL] Missing expected file: {p}")
    if p.stat().st_size < min_bytes:
        raise SystemExit(f"[E2E][FAIL] File too small/empty: {p}")


def _assert_contains(p: Path, needle: str) -> None:
    txt = p.read_text(encoding="utf-8", errors="replace")
    if needle not in txt:
        raise SystemExit(f"[E2E][FAIL] Expected text not found in {p}: {needle!r}")


def _pick_latest_spend_tx(transactions: List[Dict[str, str]], tenant_id: str, work_order_id: str) -> Optional[Dict[str, str]]:
    rows = [r for r in transactions if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id and (r.get("type") or "").upper() == "SPEND"]
    if not rows:
        return None
    # created_at is ISO Z; lexical sort works.
    rows.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    return rows[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    args = ap.parse_args()

    billing_dir = Path(args.billing_state_dir)
    runtime_dir = Path(args.runtime_dir)
    tenant_id = args.tenant_id
    work_order_id = args.work_order_id

    # ------------------------------------------------------------------
    # Discover step_ids from the workorder (step_name is UX-only)
    # ------------------------------------------------------------------
    workorder_path = Path("tenants") / tenant_id / "workorders" / f"{work_order_id}.yml"
    if not workorder_path.exists():
        raise SystemExit(f"[E2E][FAIL] Workorder YAML not found: {workorder_path}")
    wo = yaml.safe_load(workorder_path.read_text(encoding="utf-8")) or {}
    steps = wo.get("steps") or []
    if not isinstance(steps, list) or not steps:
        raise SystemExit(f"[E2E][FAIL] Workorder has no steps: {workorder_path}")

    module_to_step: Dict[str, Tuple[str, str]] = {}
    for s in steps:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("step_id") or "").strip()
        mid = str(s.get("module_id") or "").strip()
        sname = str(s.get("step_name") or s.get("name") or "").strip()
        if sid and mid:
            module_to_step[mid] = (sid, sname)

    def _sid(mid: str) -> str:
        if mid not in module_to_step:
            raise SystemExit(f"[E2E][FAIL] Workorder missing required module step: {mid}")
        return module_to_step[mid][0]

    # ------------------------------------------------------------------
    # Runtime outputs: verify chaining artifacts exist and are non-empty
    # ------------------------------------------------------------------
    base = runtime_dir / "runs" / tenant_id / work_order_id
    derive_run = _latest_run_dir(base / _sid("9SD"))
    search_run = _latest_run_dir(base / _sid("wxz"))
    seed_run = _latest_run_dir(base / _sid("U2T"))

    if not derive_run:
        raise SystemExit(f"[E2E][FAIL] Missing 9SD step output directory: {base}")

    # Step 1 must always succeed in the demo chain.
    _assert_file_exists(derive_run / "derived_queries.txt")

    # Steps 2/3 may fail in CI/offline environments (e.g., missing API keys). When that
    # happens, the orchestrator must still produce correct logs + billing refunds.
    has_search_results = bool(search_run and (search_run / "results.jsonl").exists())
    if has_search_results:
        if not seed_run:
            raise SystemExit("[E2E][FAIL] seed_text step missing output directory despite successful search")
        _assert_file_exists(search_run / "results.jsonl")
        _assert_file_exists(seed_run / "source_text.txt")
        _assert_contains(seed_run / "source_text.txt", "TOPIC:")
    else:
        # If search failed, we expect a binding_error.json or error report in seed_text.
        if seed_run:
            _assert_file_exists(seed_run / "binding_error.json")

    # ------------------------------------------------------------------
    # Billing/logging: validate module runs + transaction itemization
    # ------------------------------------------------------------------
    module_runs = _read_csv(billing_dir / "module_runs_log.csv")
    mrows = [r for r in module_runs if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id]
    if not mrows:
        raise SystemExit("[E2E][FAIL] No module_runs_log rows found for the workorder")

    statuses = {}
    for r in mrows:
        mid = (r.get("module_id") or "").strip()
        st = (r.get("status") or "").upper().strip()
        if mid:
            statuses[mid] = st

    if statuses.get("9SD") != "COMPLETED":
        raise SystemExit(f"[E2E][FAIL] Expected 9SD COMPLETED; got {statuses.get('9SD')!r}")

    # If search results exist, wxz and U2T should complete; otherwise they may fail.
    if has_search_results:
        for mid in ("wxz", "U2T"):
            if statuses.get(mid) != "COMPLETED":
                raise SystemExit(f"[E2E][FAIL] Expected {mid} COMPLETED; got {statuses.get(mid)!r}")

    transactions = _read_csv(billing_dir / "transactions.csv")
    tx = _pick_latest_spend_tx(transactions, tenant_id, work_order_id)
    if tx is None:
        raise SystemExit("[E2E][FAIL] No SPEND transaction found for the workorder")

    txid = tx.get("transaction_id") or ""
    if not txid:
        raise SystemExit("[E2E][FAIL] Latest SPEND transaction is missing transaction_id")

    items = _read_csv(billing_dir / "transaction_items.csv")
    irows = [r for r in items if r.get("transaction_id") == txid]
    if not irows:
        raise SystemExit("[E2E][FAIL] No transaction_items found for latest SPEND transaction")

    # For chaining, we require at least one RUN SPEND item per module, and refunds for failed modules.
    run_items = [r for r in irows if (r.get("feature") or "").upper() == "RUN"]
    spend_items = [r for r in run_items if (r.get("type") or "").upper() == "SPEND"]

    spend_mods = {r.get("module_id") for r in spend_items}
    missing_spend_items = [m for m in ("9SD", "wxz", "U2T") if m not in spend_mods]
    if missing_spend_items:
        raise SystemExit(f"[E2E][FAIL] Missing RUN SPEND transaction_items for modules: {missing_spend_items}")

    # Refunds are separate transactions. Validate that every FAILED module has a REFUND
    # transaction and a corresponding RUN refund item.
    all_items = _read_csv(billing_dir / "transaction_items.csv")
    refund_txs = [r for r in transactions if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id and (r.get("type") or "").upper() == "REFUND"]
    refund_mods = set()
    for r in refund_txs:
        txid2 = r.get("transaction_id") or ""
        md = (r.get("metadata_json") or "").strip()
        mid = ""
        if md:
            try:
                mid = (json.loads(md).get("module_id") or "").strip()
            except Exception:
                mid = ""
        if not mid:
            continue
        # Find a matching RUN refund item for this refund transaction.
        item_rows = [it for it in all_items if it.get("transaction_id") == txid2 and (it.get("feature") or "").upper() == "RUN" and (it.get("type") or "").upper() == "REFUND"]
        if not item_rows:
            raise SystemExit(f"[E2E][FAIL] Refund transaction missing RUN refund items: tx={txid2} module={mid}")
        refund_mods.add(mid)

    for mid in ("wxz", "U2T"):
        if statuses.get(mid) == "FAILED" and mid not in refund_mods:
            raise SystemExit(f"[E2E][FAIL] Expected REFUND transaction + item for failed module: {mid}")

    # Ensure metadata_json is parseable JSON (guards logging regressions).
    for r in (mrows[-5:] + irows[-5:]):
        mj = (r.get("metadata_json") or "").strip()
        if mj:
            try:
                json.loads(mj)
            except Exception:
                raise SystemExit("[E2E][FAIL] metadata_json is not valid JSON")

    print("[E2E][OK] Steps chaining: outputs + billing/logging validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
