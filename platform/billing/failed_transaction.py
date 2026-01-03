\
"""
Helper for writing a FAILED transaction row for unsuccessful processes (e.g., insufficient credits).
This module contains no external dependencies.

Expected transactions.csv schema (example from your verifier):
  transaction_id,tenant_id,type,status,created_at,note

Policy:
- Do NOT use "DENIED".
- For unsuccessful processes, use status="FAILED".
- type should reflect the process category (default: "RUN").
- Note should include failure reason (e.g., "Insufficient credits") and any diagnostics.

Integrate into orchestrator where you currently short-circuit due to insufficient credits.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import csv
from pathlib import Path
from typing import Optional


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def append_failed_transaction(
    transactions_csv: Path,
    transaction_id: str,
    tenant_id: str,
    process_type: str = "RUN",
    note: str = "FAILED",
    created_at: Optional[str] = None,
) -> None:
    transactions_csv.parent.mkdir(parents=True, exist_ok=True)
    created_at = created_at or utc_iso()

    # Ensure file exists with header; if not, create it.
    file_exists = transactions_csv.exists()
    with transactions_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["transaction_id","tenant_id","type","status","created_at","note"])
        w.writerow([transaction_id, tenant_id, process_type, "FAILED", created_at, note])
