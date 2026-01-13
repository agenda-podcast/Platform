from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..contracts import LedgerWriter
from ..errors import ValidationError
from ..models import TransactionItemRecord, TransactionRecord
from ...utils.csvio import read_csv


TRANSACTIONS_HEADERS = [
    "transaction_id",
    "tenant_id",
    "work_order_id",
    "type",
    "amount_credits",
    "created_at",
    "reason_code",
    "note",
    "metadata_json",
]

TRANSACTION_ITEMS_HEADERS = [
    "transaction_item_id",
    "transaction_id",
    "tenant_id",
    "module_id",
    "work_order_id",
    "step_id",
    "deliverable_id",
    "feature",
    "type",
    "amount_credits",
    "created_at",
    "note",
    "metadata_json",
]


def _ensure_csv(path: Path, headers: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        w.writeheader()


def _append_row(path: Path, headers: List[str], row: Dict[str, Any]) -> None:
    _ensure_csv(path, headers)
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore", lineterminator="\n")
        w.writerow({h: ("" if row.get(h) is None else row.get(h)) for h in headers})


def _parse_date(s: str) -> Optional[date]:
    ss = str(s or "").strip()
    if not ss:
        return None
    try:
        return datetime.strptime(ss, "%Y-%m-%d").date()
    except Exception:
        return None


class CsvLedgerWriter(LedgerWriter):
    """LedgerWriter backed by append-only .billing-state CSVs."""

    def __init__(self, state_dir: Path, repo_root: Path):
        self.state_dir = state_dir
        self.repo_root = repo_root
        self.transactions_path = state_dir / "transactions.csv"
        self.transaction_items_path = state_dir / "transaction_items.csv"
        self.prices_path = repo_root / "platform" / "billing" / "module_prices.csv"

    def post_transaction(self, tx: TransactionRecord) -> None:
        row = {
            "transaction_id": tx.transaction_id,
            "tenant_id": tx.tenant_id,
            "work_order_id": tx.work_order_id,
            "type": tx.type,
            "amount_credits": str(tx.amount_credits),
            "created_at": tx.created_at,
            "reason_code": tx.reason_code,
            "note": tx.note,
            "metadata_json": tx.metadata_json,
        }
        _append_row(self.transactions_path, TRANSACTIONS_HEADERS, row)

    def post_transaction_item(self, item: TransactionItemRecord) -> None:
        row = {
            "transaction_item_id": item.transaction_item_id,
            "transaction_id": item.transaction_id,
            "tenant_id": item.tenant_id,
            "module_id": item.module_id,
            "work_order_id": item.work_order_id,
            "step_id": item.step_id,
            "deliverable_id": item.deliverable_id,
            "feature": item.feature,
            "type": item.type,
            "amount_credits": str(item.amount_credits),
            "created_at": item.created_at,
            "note": item.note,
            "metadata_json": item.metadata_json,
        }
        _append_row(self.transaction_items_path, TRANSACTION_ITEMS_HEADERS, row)

    def append_transaction_item(self, item: TransactionItemRecord) -> None:
        self.post_transaction_item(item)

    def resolve_price(self, module_id: str, deliverable_id: str, as_of: str) -> int:
        as_of_date = _parse_date(as_of)
        if as_of_date is None:
            raise ValidationError(f"Invalid as_of date: {as_of!r} (expected YYYY-MM-DD)")

        rows = read_csv(self.prices_path)
        candidates: List[Dict[str, str]] = []
        for r in rows:
            if str(r.get("module_id", "")).strip() != module_id:
                continue
            if str(r.get("deliverable_id", "")).strip() != deliverable_id:
                continue
            active = str(r.get("active", "")).strip().lower() == "true"
            if not active:
                continue
            ef = _parse_date(r.get("effective_from", ""))
            et = _parse_date(r.get("effective_to", ""))
            if ef is not None and as_of_date < ef:
                continue
            if et is not None and as_of_date >= et:
                continue
            candidates.append(r)

        if not candidates:
            raise ValidationError(f"No active price found for {module_id}:{deliverable_id} as_of={as_of}")

        def _key(r: Dict[str, str]) -> str:
            return str(r.get("effective_from", ""))

        best = sorted(candidates, key=_key, reverse=True)[0]
        try:
            return int(str(best.get("price_credits", "0") or "0").strip() or "0")
        except Exception:
            raise ValidationError(f"Invalid price_credits for {module_id}:{deliverable_id}")

    def list_transaction_items(
        self,
        *,
        tenant_id: Optional[str] = None,
        work_order_id: Optional[str] = None,
        step_id: Optional[str] = None,
        deliverable_id: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[TransactionItemRecord]:
        rows = read_csv(self.transaction_items_path)
        out: List[TransactionItemRecord] = []
        for r in rows:
            if tenant_id and str(r.get("tenant_id", "")) != tenant_id:
                continue
            if work_order_id and str(r.get("work_order_id", "")) != work_order_id:
                continue
            if step_id and str(r.get("step_id", "")) != step_id:
                continue
            if deliverable_id and str(r.get("deliverable_id", "")) != deliverable_id:
                continue
            if since and str(r.get("created_at", "")) < since:
                continue
            try:
                amt = int(str(r.get("amount_credits", "0") or "0").strip() or "0")
            except Exception:
                amt = 0
            out.append(
                TransactionItemRecord(
                    transaction_item_id=str(r.get("transaction_item_id", "")),
                    transaction_id=str(r.get("transaction_id", "")),
                    tenant_id=str(r.get("tenant_id", "")),
                    module_id=str(r.get("module_id", "")),
                    work_order_id=str(r.get("work_order_id", "")),
                    step_id=str(r.get("step_id", "")),
                    deliverable_id=str(r.get("deliverable_id", "")),
                    feature=str(r.get("feature", "")),
                    type=str(r.get("type", "")),
                    amount_credits=amt,
                    created_at=str(r.get("created_at", "")),
                    note=str(r.get("note", "")),
                    metadata_json=str(r.get("metadata_json", "")),
                )
            )
        return out
