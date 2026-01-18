# Generated. Do not edit by hand.
CHUNK = r'''\
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..billing.state import BillingState
from ..orchestration.module_exec import execute_module_runner
from ..secretstore.loader import load_secretstore, env_for_module
from ..utils.csvio import read_csv, write_csv
from ..utils.time import utcnow_iso
from ..utils.yamlio import read_yaml

from .config import RuntimeProfile
from .contracts import (
    ArtifactPublisher,
    ArtifactStore,
    ExecutionBackend,
    LedgerWriter,
    ModuleRegistry,
    RunStateStore,
)
from .errors import NotConfiguredError, ValidationError
from .models import (
    MODULE_KIND_VALUES,
    is_valid_module_kind,
    DeliverableArtifactRecord,
    OutputRecord,
    StepRunRecord,
    StepSpec,
    TransactionItemRecord,
    WorkorderSpec,
)


@dataclass
class InfraBundle:
    profile: RuntimeProfile
    registry: ModuleRegistry
    run_state_store: RunStateStore
    ledger_writer: LedgerWriter
    artifact_store: ArtifactStore
    execution_backend: ExecutionBackend
    artifact_publisher: ArtifactPublisher

    tenant_credentials_store: TenantCredentialsStore | None = None

    # ---------------------------------------------------------------------
    # Backward-compatible attribute aliases
    #
    # Earlier iterations of the repository exposed adapter instances under
    # shorter names (infra.ledger, infra.run_state, infra.artifacts). Several
    # scripts (including offline verification helpers) still rely on that API.
    #
    # Keep these aliases to avoid hard failures, while the canonical fields
    # remain: ledger_writer, run_state_store, artifact_store.
    # ---------------------------------------------------------------------

    @property
    def ledger(self) -> LedgerWriter:
        return self.ledger_writer

    @property
    def run_state(self) -> RunStateStore:
        return self.run_state_store

    @property
    def artifacts(self) -> ArtifactStore:
        return self.artifact_store

    def describe(self) -> Dict[str, Any]:
        def _d(x: Any) -> Dict[str, Any]:
            if hasattr(x, "describe") and callable(getattr(x, "describe")):
                return dict(getattr(x, "describe")())
            return {"class": x.__class__.__name__}

        return {
            "profile_name": self.profile.profile_name,
            "adapters": {
                "registry": _d(self.registry),
                "run_state_store": _d(self.run_state_store),
                "ledger_writer": _d(self.ledger_writer),
                "artifact_store": _d(self.artifact_store),
                "execution_backend": _d(self.execution_backend),
                "artifact_publisher": _d(self.artifact_publisher),
                "tenant_credentials_store": _d(self.tenant_credentials_store) if self.tenant_credentials_store is not None else {"class": "NotConfigured"},
            },
        }


class RepoCsvModuleRegistry:
    def __init__(self, repo_root: Path, modules_csv: Optional[Path] = None) -> None:
        self.repo_root = repo_root
        self.modules_csv = modules_csv or (repo_root / "platform" / "modules" / "modules.csv")

    def list_modules(self) -> List[str]:
        if self.modules_csv.exists():
            rows = read_csv(self.modules_csv)
            out: List[str] = []
            for r in rows:
                mid = str(r.get("module_id", "") or "").strip()
                if mid:
                    out.append(mid)
            return out

        modules_dir = self.repo_root / "modules"
        if not modules_dir.exists():
            return []
        return [p.name for p in sorted(modules_dir.iterdir(), key=lambda p: p.name) if p.is_dir()]

    def module_path(self, module_id: str) -> Path:
        return self.repo_root / "modules" / str(module_id).strip()

    def load_module_yaml(self, module_id: str) -> Dict[str, Any]:
        p = self.module_path(module_id) / "module.yml"
        if not p.exists():
            raise ValidationError(f"module.yml not found for module_id={module_id!r}: {p}")
        data = read_yaml(p) or {}
        if not isinstance(data, dict):
            raise ValidationError(f"Invalid module.yml format for module_id={module_id!r}: {p}")

        kind = str(data.get("kind") or "").strip()
        if not kind:
            raise ValidationError(
                f"module.yml missing required field 'kind' for module_id={module_id!r} (allowed: {list(MODULE_KIND_VALUES)})"
            )
        if not is_valid_module_kind(kind):
            raise ValidationError(
                f"module.yml has invalid kind={kind!r} for module_id={module_id!r} (allowed: {list(MODULE_KIND_VALUES)})"
            )
        return data

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "modules_csv": str(self.modules_csv),
        }




class PostgresModuleRegistry:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def list_modules(self):
        raise NotImplementedError

    def module_path(self, module_id: str):
        raise NotImplementedError

    def load_module_yaml(self, module_id: str):
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "dsn": self.dsn}


class PostgresRunStateStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def append_step_run(self, record: StepRunRecord) -> None:
        raise NotImplementedError

    def list_step_runs(self, tenant_id: str, work_order_id: str):
        raise NotImplementedError

    def append_output(self, record: OutputRecord) -> None:
        raise NotImplementedError

    def list_outputs(self, tenant_id: str, work_order_id: str, step_id: str):
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "dsn": self.dsn}


class PostgresLedgerWriter:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def append_transaction_item(self, item: TransactionItemRecord) -> None:
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "dsn": self.dsn}


class S3ArtifactStorePlaceholder:
    """Placeholder for the future platform-native S3 integration.

    The actual ArtifactStore implementation lives in platform.infra.adapters.artifacts_s3.
    This placeholder remains only to preserve earlier scaffolding structure.
    """

    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix

    def put_bytes(self, key: str, data: bytes, content_type: str = "") -> None:
        raise NotImplementedError

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def list_keys(self, prefix: str = ""):
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "bucket": self.bucket, "prefix": self.prefix}


class ExternalEngineExecutionBackend:
    def __init__(self, endpoint: str, timeout_seconds: int = 900) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = int(timeout_seconds)

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

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "endpoint": self.endpoint, "timeout_seconds": self.timeout_seconds}


class CloudStorageArtifactPublisher:
    def __init__(self, target: str, bucket: str, prefix: str = "") -> None:
        self.target = target
        self.bucket = bucket
        self.prefix = prefix

    def publish_deliverable(
        self,
        *,
        repo_root: Path,
        record: DeliverableArtifactRecord,
        local_path: Path,
        content_type: str = "application/zip",
    ) -> DeliverableArtifactRecord:
        raise NotImplementedError

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__, "target": self.target, "bucket": self.bucket, "prefix": self.prefix}


class TenantCredentialsStorePlaceholder:
    """Placeholder for tenant credentials resolution.

    Declared so runtime profiles can reference tenant_credentials_store without breaking dev mode.
    Concrete implementations will be wired for production runtimes (DB-backed, secretstore-backed, etc.).
    """

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def get_integration(self, tenant_id: str, provider: str) -> Optional[Dict[str, Any]]:
        raise NotConfiguredError(
            f"tenant_credentials_store not wired: kind={self.kind!r} tenant_id={tenant_id!r} provider={provider!r}"
        )

    def upsert_integration(self, *, tenant_id: str, provider: str, integration: Dict[str, Any]) -> None:
        _ = integration
        raise NotConfiguredError(
            f"tenant_credentials_store not wired: kind={self.kind!r} tenant_id={tenant_id!r} provider={provider!r}"
        )

    def describe(self) -> dict[str, object]:
        return {"class": self.__class__.__name__, "kind": self.kind}

class BillingStateCsvRunStateStore:
    """Run state store backed by billing-state CSVs.

    Notes:
      - This adapter currently stores only step run records in module_runs_log.csv.
      - OutputRecord methods are implemented as no-ops because the platform does not yet
        have a canonical outputs index table.
    """

    MODULE_RUNS_HEADERS = [
        "module_run_id",
        "tenant_id",
        "work_order_id",
        "module_id",
        "status",
        "created_at",
        "started_at",
        "ended_at",
        "reason_code",
        "report_path",
        "output_ref",
        "metadata_json",
    ]

    def __init__(self, billing_state_dir: Path) -> None:
        self.billing = BillingState(billing_state_dir)

    def append_step_run(self, record: StepRunRecord) -> None:
        p = self.runtime_dir / "runstate" / "module_runs_log.csv"
        rows = read_csv(p)
        rows.append(
            {
                "module_run_id": record.module_run_id,
                "tenant_id": record.tenant_id,
                "work_order_id": record.work_order_id,
                "module_id": record.module_id,
                "status": record.status,
                "created_at": record.created_at,
                "started_at": record.started_at,
                "ended_at": record.ended_at,
                "reason_code": record.reason_code,
                "report_path": record.report_path,
                "output_ref": record.output_ref,
                "metadata_json": json.dumps(record.metadata, ensure_ascii=False, separators=(",", ":")),
            }
        )
        write_csv(p, rows, self.MODULE_RUNS_HEADERS)

    def list_step_runs(self, tenant_id: str, work_order_id: str) -> List[StepRunRecord]:
        p = self.runtime_dir / "runstate" / "module_runs_log.csv"
        rows = read_csv(p)
        out: List[StepRunRecord] = []
        for r in rows:
            if str(r.get("tenant_id", "")) != tenant_id:
                continue
            if str(r.get("work_order_id", "")) != work_order_id:
                continue
            md = str(r.get("metadata_json", "") or "").strip()
            meta: Dict[str, Any] = {}
            if md:
                try:
                    meta = json.loads(md)
                except Exception:
                    meta = {}

            out.append(
                StepRunRecord(
                    module_run_id=str(r.get("module_run_id", "") or ""),
                    tenant_id=str(r.get("tenant_id", "") or ""),
                    work_order_id=str(r.get("work_order_id", "") or ""),
                    step_id=str(meta.get("step_id", "") or ""),
                    module_id=str(r.get("module_id", "") or ""),
                    status=str(r.get("status", "") or ""),
                    created_at=str(r.get("created_at", "") or ""),
                    started_at=str(r.get("started_at", "") or ""),
                    ended_at=str(r.get("ended_at", "") or ""),
                    reason_code=str(r.get("reason_code", "") or ""),
                    output_ref=str(r.get("output_ref", "") or ""),
                    report_path=str(r.get("report_path", "") or ""),
                    metadata=meta,
                )
            )
        return out

    def append_output(self, record: OutputRecord) -> None:
        _ = record
        return None

    def list_outputs(self, tenant_id: str, work_order_id: str, step_id: str) -> List[OutputRecord]:
        _ = (tenant_id, work_order_id, step_id)
        return []

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "billing_state_dir": str(self.billing.root),
            "module_runs_log": str(self.runtime_dir / "runstate" / "module_runs_log.csv"),
        }

'''

def get_part() -> str:
    return CHUNK
