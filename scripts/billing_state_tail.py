#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _parse_iso_z(s: str) -> Optional[datetime]:
    """Parse ISO timestamps that may end with 'Z'. Returns UTC-aware datetime or None."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1] + "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _row_best_ts(row: dict[str, str]) -> Optional[datetime]:
    """Pick the best per-row timestamp to decide whether it's from this run."""
    for k in ("ended_at", "started_at", "created_at"):
        dt = _parse_iso_z(row.get(k, ""))
        if dt is not None:
            return dt
    return None


@dataclass
class Lookups:
    reason_by_code: dict[str, str]
    module_name_by_id: dict[str, str]
    workorder_by_transaction_id: dict[str, str]


def _load_reason_catalog(repo_root: Path) -> dict[str, str]:
    """Loads maintenance-state/reason_catalog.csv if available."""
    path = repo_root / "maintenance-state" / "reason_catalog.csv"
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            code = (r.get("reason_code") or "").strip()
            name = (r.get("name") or r.get("title") or r.get("description") or "").strip()
            if code:
                out[code] = name or code
    return out


def _load_module_names(repo_root: Path) -> dict[str, str]:
    """Load optional module display names from modules/*/module.yml (if present)."""
    out: dict[str, str] = {}
    modules_dir = repo_root / "modules"
    if not modules_dir.exists():
        return out
    # We avoid importing PyYAML here; module.yml is optional for tail prettification.
    for d in sorted(modules_dir.iterdir()):
        if not d.is_dir():
            continue
        mid = d.name
        yml = d / "module.yml"
        if not yml.exists():
            continue
        # Minimal YAML parsing: look for a top-level `name:` or fallback to `description:` first line.
        name: Optional[str] = None
        desc: Optional[str] = None
        try:
            for line in yml.read_text(encoding="utf-8").splitlines():
                if line.startswith("name:"):
                    name = line.split(":", 1)[1].strip().strip('"').strip("'")
                    break
                if line.startswith("description:") and desc is None:
                    desc = line.split(":", 1)[1].strip().strip('"').strip("'")
        except Exception:
            continue
        if name:
            out[mid] = name
        elif desc:
            out[mid] = desc
    return out


def _load_lookups(repo_root: Path) -> Lookups:
    return Lookups(
        reason_by_code=_load_reason_catalog(repo_root),
        module_name_by_id=_load_module_names(repo_root),
        workorder_by_transaction_id={},
    )


def _format_compact_row(path: Path, row: dict[str, str], lookups: Lookups) -> str:
    """Produce a human-readable compact line for logs."""
    fname = path.name

    # Resolve a few common enrichments
    enriched = dict(row)
    rc = (row.get("reason_code") or "").strip()
    if rc:
        enriched["reason"] = lookups.reason_by_code.get(rc, rc)
    mid = (row.get("module_id") or "").strip()
    if mid:
        enriched["module"] = lookups.module_name_by_id.get(mid, mid)

    if fname == "transaction_items.csv":
        # Back-compat: older billing-state templates may not include work_order_id on items.
        if not (row.get("work_order_id") or "").strip():
            txid = (row.get("transaction_id") or "").strip()
            wo = lookups.workorder_by_transaction_id.get(txid, "") if txid else ""
            if wo:
                enriched["work_order_id"] = wo

    # Prefer showing note if present
    keys_preferred = []
    if fname == "transactions.csv":
        keys_preferred = ["transaction_id", "tenant_id", "work_order_id", "type", "amount_credits", "created_at", "note"]
    elif fname == "transaction_items.csv":
        # include module_id (schema) and infer work_order_id via transactions.csv join
        keys_preferred = ["transaction_item_id", "transaction_id", "tenant_id", "work_order_id", "step_id", "module_id", "module", "deliverable_id", "feature", "type", "amount_credits", "created_at", "note"]
    elif fname == "cache_index.csv":
        keys_preferred = ["place", "type", "ref", "created_at", "expires_at"]

    else:
        # generic
        keys_preferred = list(enriched.keys())[:8]

    parts: list[str] = []
    for k in keys_preferred:
        if k not in enriched:
            continue
        v = (enriched.get(k) or "").strip()
        if not v:
            continue
        # keep metadata_json out of logs by default
        if k == "metadata_json":
            continue
        parts.append(f"{k}={v}")

    return "- " + ", ".join(parts)


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _filter_rows_for_run(rows: list[dict[str, str]], since: Optional[datetime]) -> list[dict[str, str]]:
    if since is None:
        return rows
    out: list[dict[str, str]] = []
    for r in rows:
        dt = _row_best_ts(r)
        if dt is None:
            continue
        if dt >= since:
            out.append(r)
    return out


def print_table(path: Path, n: int, since: Optional[datetime], lookups: Lookups) -> None:
    print("\n" + "=" * 100)
    print(f"{path.name}  (exists={path.exists()})")
    if not path.exists():
        print("<missing>")
        return
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
    print(f"header: {header}")

    rows_all = _read_rows(path)
    rows = _filter_rows_for_run(rows_all, since)
    # Only show the last N of *filtered* rows
    rows_tail = rows[-n:]
    print(f"rows_new={len(rows)} (showing_last={len(rows_tail)})")
    for r in rows_tail:
        print(_format_compact_row(path, r, lookups))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--since", default="", help="Only show rows with started_at/ended_at/created_at >= this ISO time (UTC).")
    args = ap.parse_args()

    bdir = Path(args.billing_state_dir).resolve()
    repo_root = Path(__file__).resolve().parents[1]
    lookups = _load_lookups(repo_root)

    # Build transaction_id -> work_order_id map for enriching transaction_items.csv output
    try:
        tx_rows = _read_rows(bdir / "transactions.csv")
        lookups.workorder_by_transaction_id = {
            (r.get("transaction_id") or "").strip(): (r.get("work_order_id") or "").strip()
            for r in tx_rows
            if (r.get("transaction_id") or "").strip() and (r.get("work_order_id") or "").strip()
        }
    except Exception:
        lookups.workorder_by_transaction_id = {}

    since_dt = _parse_iso_z(args.since) if args.since else None
    print("\n[BILLING_STATE_TAIL] billing_state_dir=", bdir)
    if since_dt is not None:
        print("[BILLING_STATE_TAIL] since=", since_dt.isoformat().replace("+00:00", "Z"))

    for fname in [
                        "transactions.csv",
        "transaction_items.csv",
        "tenants_credits.csv",
        "cache_index.csv",
        "github_releases_map.csv",
        "github_assets_map.csv",
    ]:
        print_table(bdir / fname, args.n, since_dt, lookups)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
