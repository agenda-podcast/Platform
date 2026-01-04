\
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _parse_iso(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    ts = ts.strip()
    # Support "Z"
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return ([], [])
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    return (cols, rows)


def _row_time(row: Dict[str, str], candidates: List[str]) -> Optional[datetime]:
    for c in candidates:
        dt = _parse_iso(row.get(c, "") or "")
        if dt:
            return dt
    return None


def _filter_since(rows: List[Dict[str, str]], since: Optional[datetime], time_cols: List[str]) -> List[Dict[str, str]]:
    if since is None:
        return rows
    out: List[Dict[str, str]] = []
    for r in rows:
        dt = _row_time(r, time_cols)
        if dt and dt >= since:
            out.append(r)
    return out


def _tail(rows: List[Dict[str, str]], n: int) -> List[Dict[str, str]]:
    if n <= 0:
        return []
    return rows[-n:] if len(rows) > n else rows


def _print_table(title: str, cols: List[str], rows: List[Dict[str, str]], max_rows: int = 20) -> None:
    print("")
    print("=" * 90)
    print(title)
    print("=" * 90)
    if not rows:
        print("(no rows for this run)")
        return

    rows = _tail(rows, max_rows)
    # widths
    widths = {c: len(c) for c in cols}
    for r in rows:
        for c in cols:
            widths[c] = max(widths[c], len(str(r.get(c, "") or "")))

    def fmt_row(r: Dict[str, str]) -> str:
        return " | ".join(str(r.get(c, "") or "").ljust(widths[c]) for c in cols)

    print(" | ".join(c.ljust(widths[c]) for c in cols))
    print("-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print(fmt_row(r))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", default=".billing-state")
    ap.add_argument("--runtime-dir", default="runtime")
    ap.add_argument("--tenants-dir", default="tenants")
    ap.add_argument("--since", default="", help="UTC ISO time (e.g., 2026-01-03T23:48:06Z). If set, only rows created/started after this time are shown.")
    ap.add_argument("--max-rows", type=int, default=25)
    args = ap.parse_args()

    since = _parse_iso(args.since) if args.since else None
    bdir = Path(args.billing_state_dir)
    rdir = Path(args.runtime_dir)
    tdir = Path(args.tenants_dir)

    print("")
    print("ORCHESTRATOR RUN-SCOPED SUMMARY")
    print(f"billing_state_dir: {bdir.resolve()}")
    print(f"runtime_dir:       {rdir.resolve()}")
    print(f"tenants_dir:       {tdir.resolve()}")
    print(f"since:             {since.isoformat().replace('+00:00','Z') if since else '(not set)'}")

    # Workorders discovered
    workorders = sorted(tdir.glob("*/workorders/*.yml"))
    print("")
    print(f"Discovered workorders: {len(workorders)}")
    for p in workorders[:50]:
        print(f" - {p}")

    # Billing-state logs (filtered to this run)
    # workorders_log.csv
    wo_cols, wo_rows = _read_csv(bdir / "workorders_log.csv")
    wo_filtered = _filter_since(wo_rows, since, ["started_at", "ended_at", "completed_at", "created_at"])
    _print_table(
        f"Workorders Log (this run only) - showing {min(len(wo_filtered), args.max_rows)}/{len(wo_filtered)} rows (filtered from {len(wo_rows)})",
        wo_cols if wo_cols else ["work_order_id","tenant_id","status","created_at","started_at","ended_at","note","metadata_json"],
        wo_filtered,
        max_rows=args.max_rows,
    )

    # module_runs_log.csv
    mr_cols, mr_rows = _read_csv(bdir / "module_runs_log.csv")
    mr_filtered = _filter_since(mr_rows, since, ["started_at", "ended_at", "created_at"])
    _print_table(
        f"Module Runs Log (this run only) - showing {min(len(mr_filtered), args.max_rows)}/{len(mr_filtered)} rows (filtered from {len(mr_rows)})",
        mr_cols if mr_cols else ["run_id","work_order_id","tenant_id","module_id","status","started_at","ended_at","credits_charged","note","metadata_json"],
        mr_filtered,
        max_rows=args.max_rows,
    )

    # transactions.csv
    tx_cols, tx_rows = _read_csv(bdir / "transactions.csv")
    tx_filtered = _filter_since(tx_rows, since, ["created_at"])
    _print_table(
        f"Transactions (this run only) - showing {min(len(tx_filtered), args.max_rows)}/{len(tx_filtered)} rows (filtered from {len(tx_rows)})",
        tx_cols if tx_cols else ["transaction_id","tenant_id","type","status","created_at","note"],
        tx_filtered,
        max_rows=args.max_rows,
    )

    # transaction_items.csv
    ti_cols, ti_rows = _read_csv(bdir / "transaction_items.csv")
    ti_filtered = _filter_since(ti_rows, since, ["created_at"])
    _print_table(
        f"Transaction Items (this run only) - showing {min(len(ti_filtered), args.max_rows)}/{len(ti_filtered)} rows (filtered from {len(ti_rows)})",
        ti_cols if ti_cols else ["transaction_item_id","transaction_id","tenant_id","work_order_id","module_id","feature_id","quantity","amount_credits","created_at","metadata_json"],
        ti_filtered,
        max_rows=args.max_rows,
    )

    # Runtime snapshot (top-level only)
    print("")
    print("Runtime directory snapshot (top-level):")
    if rdir.exists():
        for p in sorted(rdir.glob("*")):
            if p.is_dir():
                print(f" - {p.name}/")
            else:
                print(f" - {p.name} ({p.stat().st_size} bytes)")
    else:
        print(" (runtime dir not found)")

    # Tenant outputs snapshot
    print("")
    print("Tenant outputs snapshot:")
    if tdir.exists():
        for tenant in sorted([p for p in tdir.iterdir() if p.is_dir()])[:100]:
            outdir = tenant / "outputs"
            if outdir.exists():
                items = list(outdir.rglob("*"))
                print(f" - {tenant.name}: outputs present ({len(items)} items)")
            else:
                print(f" - {tenant.name}: no outputs/ directory")
    else:
        print(" (tenants dir not found)")
    print("")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
