from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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
    # Runtime outputs: verify chaining artifacts exist and are non-empty
    # ------------------------------------------------------------------
    base = runtime_dir / "runs" / tenant_id / work_order_id
    derive_run = _latest_run_dir(base / "derive_queries")
    search_run = _latest_run_dir(base / "search")
    seed_run = _latest_run_dir(base / "seed_text")

    if not (derive_run and search_run and seed_run):
        raise SystemExit(
            "[E2E][FAIL] Expected step output directories not found. "
            f"derive={derive_run}, search={search_run}, seed={seed_run}"
        )

    _assert_file_exists(derive_run / "derived_queries.txt")
    _assert_file_exists(search_run / "results.jsonl")
    _assert_file_exists(seed_run / "source_text.txt")
    _assert_contains(seed_run / "source_text.txt", "TOPIC:")

    # ------------------------------------------------------------------
    # Billing/logging: validate module runs + transaction itemization
    # ------------------------------------------------------------------
    module_runs = _read_csv(billing_dir / "module_runs_log.csv")
    mrows = [r for r in module_runs if r.get("tenant_id") == tenant_id and r.get("work_order_id") == work_order_id]
    if not mrows:
        raise SystemExit("[E2E][FAIL] No module_runs_log rows found for the workorder")

    completed_mods = {r.get("module_id") for r in mrows if (r.get("status") or "").upper() == "COMPLETED"}
    missing = [m for m in ("9SD", "wxz", "U2T") if m not in completed_mods]
    if missing:
        raise SystemExit(f"[E2E][FAIL] Missing COMPLETED module runs for: {missing}")

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

    # For chaining, we require at least one RUN item per module.
    run_items = [r for r in irows if (r.get("feature") or "").upper() == "RUN"]
    run_mods = {r.get("module_id") for r in run_items}
    missing_items = [m for m in ("9SD", "wxz", "U2T") if m not in run_mods]
    if missing_items:
        raise SystemExit(f"[E2E][FAIL] Missing RUN transaction_items for modules: {missing_items}")

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
