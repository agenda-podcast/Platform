from __future__ import annotations

import unittest

from _testutil import ensure_repo_on_path


class TestInfraContracts(unittest.TestCase):
    def test_contract_protocols_importable(self) -> None:
        ensure_repo_on_path()

        from typing import Protocol

        from platform.infra.contracts import (
            ArtifactPublisher,
            ChecksumComputer,
            ArtifactStore,
            ExecutionBackend,
            LedgerWriter,
            ModuleRegistry,
            RunStateStore,
            WorkorderValidator,
            TenantCredentialsStore,
        )

        for cls in (ModuleRegistry, RunStateStore, LedgerWriter, ArtifactStore, ExecutionBackend, ArtifactPublisher, ChecksumComputer, WorkorderValidator, TenantCredentialsStore):
            self.assertTrue(issubclass(cls, Protocol))


if __name__ == "__main__":
    unittest.main()
