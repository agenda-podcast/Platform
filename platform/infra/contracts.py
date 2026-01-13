from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Tuple

from .models import (
    DeliverableArtifactRecord,
    OutputRecord,
    StepRunRecord,
    StepSpec,
    TransactionItemRecord,
    TransactionRecord,
    WorkorderSpec,
)


class ModuleRegistry(Protocol):
    def list_modules(self) -> List[str]:
        raise NotImplementedError

    def module_path(self, module_id: str) -> Path:
        raise NotImplementedError

    def load_module_yaml(self, module_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def get_contract(self, module_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def list_deliverables(self, module_id: str) -> List[str]:
        raise NotImplementedError

    def get_deliverable(self, module_id: str, deliverable_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def validate_workorder(self, repo_root: Path, workorder_path: Path) -> None:
        raise NotImplementedError


class RunStateStore(Protocol):
    def create_run(self, tenant_id: str, work_order_id: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        raise NotImplementedError

    def create_step_run(
        self,
        *,
        tenant_id: str,
        work_order_id: str,
        step_id: str,
        module_id: str,
        idempotency_key: str,
        outputs_dir: Optional[Path] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepRunRecord:
        raise NotImplementedError

    def mark_step_run_running(self, module_run_id: str, metadata: Optional[Dict[str, Any]] = None) -> StepRunRecord:
        raise NotImplementedError

    def mark_step_run_succeeded(
        self,
        module_run_id: str,
        *,
        requested_deliverables: List[str],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepRunRecord:
        raise NotImplementedError

    def mark_step_run_failed(self, module_run_id: str, error: Dict[str, Any]) -> StepRunRecord:
        raise NotImplementedError

    def record_output(self, record: OutputRecord) -> None:
        raise NotImplementedError

    def list_outputs(self, tenant_id: str, work_order_id: str, step_id: str) -> List[OutputRecord]:
        raise NotImplementedError

    def get_output(self, tenant_id: str, work_order_id: str, step_id: str, output_id: str) -> OutputRecord:
        raise NotImplementedError

    def list_step_runs(self, tenant_id: str, work_order_id: str) -> List[StepRunRecord]:
        raise NotImplementedError

    def set_run_status(self, tenant_id: str, work_order_id: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError


    def record_deliverable_artifact(self, record: DeliverableArtifactRecord) -> None:
        raise NotImplementedError

    def list_deliverable_artifacts(self, *, tenant_id: str, work_order_id: str) -> List[DeliverableArtifactRecord]:
        raise NotImplementedError

    # Legacy alias methods
    def append_step_run(self, record: StepRunRecord) -> None:
        raise NotImplementedError

    def append_output(self, record: OutputRecord) -> None:
        raise NotImplementedError


class LedgerWriter(Protocol):
    def post_transaction(self, tx: TransactionRecord) -> None:
        raise NotImplementedError

    def post_transaction_item(self, item: TransactionItemRecord) -> None:
        raise NotImplementedError

    def resolve_price(self, module_id: str, deliverable_id: str, as_of: str) -> int:
        raise NotImplementedError

    def list_transaction_items(
        self,
        *,
        tenant_id: Optional[str] = None,
        work_order_id: Optional[str] = None,
        step_id: Optional[str] = None,
        deliverable_id: Optional[str] = None,
        since: Optional[str] = None,
    ) -> List[TransactionItemRecord]:
        raise NotImplementedError

    # Legacy alias method
    def append_transaction_item(self, item: TransactionItemRecord) -> None:
        raise NotImplementedError


class ArtifactStore(Protocol):
    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        raise NotImplementedError

    def get_to_file(self, key: str, dest_path: Path) -> None:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def list_keys(self, prefix: str = "") -> List[str]:
        raise NotImplementedError


class ExecutionBackend(Protocol):
    def execute_step(
        self,
        *,
        repo_root: Path,
        workorder: WorkorderSpec,
        step: StepSpec,
        outputs_dir: Path,
        module_path: Optional[Path] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> Tuple[StepRunRecord, List[OutputRecord], Dict[str, Any]]:
        raise NotImplementedError


class ArtifactPublisher(Protocol):
    def publish_deliverable(
        self,
        *,
        repo_root: Path,
        record: DeliverableArtifactRecord,
        local_path: Path,
        content_type: str = "application/zip",
    ) -> DeliverableArtifactRecord:
        raise NotImplementedError


class ChecksumComputer(Protocol):
    """Optional utility for computing checksums and sizes for local artifacts.

    This is deliberately small so packaging/publishing code can depend on a stable interface
    without forcing every runtime to provide an implementation.
    """

    def sha256_file(self, path: Path) -> str:
        raise NotImplementedError

    def file_size_bytes(self, path: Path) -> int:
        raise NotImplementedError


class WorkorderValidator(Protocol):
    """Optional validator for workorders and step configurations.

    The platform already contains validators; this interface exists to decouple callers
    from the concrete location of validation logic.
    """

    def validate(self, *, repo_root: Path, workorder_path: Path) -> None:
        raise NotImplementedError


class TenantCredentialsStore(Protocol):
    """Store for tenant integrations/credentials.

    This is intentionally generic: providers can include email, dropbox, s3, etc.
    Implementations are environment-specific (CSV for dev, DB/secretstore for prod).
    """

    def get_integration(self, tenant_id: str, provider: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def upsert_integration(self, *, tenant_id: str, provider: str, integration: Dict[str, Any]) -> None:
        raise NotImplementedError
