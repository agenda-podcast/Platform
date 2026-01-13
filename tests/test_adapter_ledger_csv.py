from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestCsvLedgerWriter(unittest.TestCase):
    def test_append_and_headers(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.ledger_csv import CsvLedgerWriter, TRANSACTIONS_HEADERS, TRANSACTION_ITEMS_HEADERS
        from platform.infra.models import TransactionItemRecord, TransactionRecord

        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td)

            # Minimal repo_root for pricing lookups is not needed for this test.
            repo_root = state_dir
            (repo_root / "platform" / "billing").mkdir(parents=True, exist_ok=True)
            (repo_root / "platform" / "billing" / "module_prices.csv").write_text(
                "module_id,deliverable_id,price_credits,effective_from,effective_to,active,notes\n",
                encoding="utf-8",
            )

            w = CsvLedgerWriter(state_dir=state_dir, repo_root=repo_root)

            tx = TransactionRecord(
                transaction_id="tx000001",
                tenant_id="t1",
                work_order_id="w1",
                type="SPEND",
                amount_credits=3,
                created_at="2026-01-01T00:00:00Z",
            )
            w.post_transaction(tx)

            item = TransactionItemRecord(
                transaction_item_id="ti000001",
                transaction_id="tx000001",
                tenant_id="t1",
                module_id="m1",
                work_order_id="w1",
                step_id="s1",
                deliverable_id="__run__",
                feature="run",
                type="SPEND",
                amount_credits=3,
                created_at="2026-01-01T00:00:00Z",
            )
            w.post_transaction_item(item)

            with (state_dir / "transactions.csv").open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(reader.fieldnames, TRANSACTIONS_HEADERS)
                self.assertEqual(len(rows), 1)

            with (state_dir / "transaction_items.csv").open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(reader.fieldnames, TRANSACTION_ITEMS_HEADERS)
                self.assertEqual(len(rows), 1)

    def test_effective_dated_pricing(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.ledger_csv import CsvLedgerWriter

        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td) / "repo"
            state_dir = Path(td) / "state"
            (repo_root / "platform" / "billing").mkdir(parents=True, exist_ok=True)
            prices_csv = repo_root / "platform" / "billing" / "module_prices.csv"
            prices_csv.write_text(
                "module_id,deliverable_id,price_credits,effective_from,effective_to,active,notes\n"
                "m1,__run__,5,2020-01-01,2024-01-01,true,old\n"
                "m1,__run__,7,2024-01-01,,true,new\n"
                "m1,__run__,9,2024-01-01,,false,inactive\n",
                encoding="utf-8",
            )

            w = CsvLedgerWriter(state_dir=state_dir, repo_root=repo_root)
            self.assertEqual(w.resolve_price("m1", "__run__", "2023-12-31"), 5)
            self.assertEqual(w.resolve_price("m1", "__run__", "2024-01-01"), 7)


if __name__ == "__main__":
    unittest.main()
