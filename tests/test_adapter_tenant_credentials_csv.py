from __future__ import annotations

import csv
import os
import tempfile
import unittest
from pathlib import Path

from _testutil import ensure_repo_on_path


class TestTenantCredentialsStoreCsv(unittest.TestCase):
    def test_upsert_encrypt_and_get(self) -> None:
        ensure_repo_on_path()

        from platform.infra.adapters.tenant_credentials_csv import TenantCredentialsStoreCsv

        old = os.environ.get("TOKEN_ENCRYPTION_KEY")
        os.environ["TOKEN_ENCRYPTION_KEY"] = "test-passphrase"

        try:
            with tempfile.TemporaryDirectory() as td:
                repo_root = Path(td)
                tenants_root = repo_root / "tenants"
                store = TenantCredentialsStoreCsv(repo_root=repo_root, tenants_root=tenants_root)

                store.upsert_integration(
                    tenant_id="t1",
                    provider="dropbox",
                    integration={
                        "status": "active",
                        "note": "n1",
                        "token": {"refresh_token": "r1", "access_token": "a1"},
                    },
                )

                csv_path = tenants_root / "t1" / "integrations" / "tenant_integrations.csv"
                self.assertTrue(csv_path.exists())

                with csv_path.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)

                self.assertEqual(len(rows), 1)
                self.assertEqual(str(rows[0].get("provider")), "dropbox")
                self.assertNotIn("token", str(rows[0].get("integration_json", "")))

                tokens_path = tenants_root / "t1" / "integrations" / "tokens.gpg"
                self.assertTrue(tokens_path.exists())

                got = store.get_integration("t1", "dropbox")
                self.assertIsNotNone(got)
                self.assertEqual(got.get("provider"), "dropbox")
                self.assertEqual(got.get("token", {}).get("refresh_token"), "r1")

                # Update without token must preserve existing token
                store.upsert_integration(
                    tenant_id="t1",
                    provider="dropbox",
                    integration={
                        "status": "active",
                        "note": "n2",
                    },
                )
                got2 = store.get_integration("t1", "dropbox")
                self.assertIsNotNone(got2)
                self.assertEqual(got2.get("token", {}).get("refresh_token"), "r1")

                # Update with new token must replace
                store.upsert_integration(
                    tenant_id="t1",
                    provider="dropbox",
                    integration={
                        "status": "active",
                        "note": "n3",
                        "token": {"refresh_token": "r2"},
                    },
                )
                got3 = store.get_integration("t1", "dropbox")
                self.assertIsNotNone(got3)
                self.assertEqual(got3.get("token", {}).get("refresh_token"), "r2")
        finally:
            if old is None:
                os.environ.pop("TOKEN_ENCRYPTION_KEY", None)
            else:
                os.environ["TOKEN_ENCRYPTION_KEY"] = old


if __name__ == "__main__":
    unittest.main()
