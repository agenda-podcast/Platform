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
    # scripts (including E2E verification helpers) still rely on that API.
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
        p = self.billing.path("module_runs_log.csv")
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
        p = self.billing.path("module_runs_log.csv")
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
            "module_runs_log": str(self.billing.path("module_runs_log.csv")),
        }


class BillingStateCsvLedgerWriter:
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

    def __init__(self, billing_state_dir: Path) -> None:
        self.billing = BillingState(billing_state_dir)

    def append_transaction_item(self, item: TransactionItemRecord) -> None:
        p = self.billing.path("transaction_items.csv")
        rows = read_csv(p)
        rows.append(
            {
                "transaction_item_id": item.transaction_item_id,
                "transaction_id": item.transaction_id,
                "tenant_id": item.tenant_id,
                "module_id": item.module_id,
                "work_order_id": item.work_order_id,
                "step_id": item.step_id,
                "deliverable_id": item.deliverable_id,
                "feature": item.feature,
                "type": item.type,
                "amount_credits": str(int(item.amount_credits)),
                "created_at": item.created_at,
                "note": item.note,
                "metadata_json": item.metadata_json,
            }
        )
        write_csv(p, rows, self.TRANSACTION_ITEMS_HEADERS)

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "billing_state_dir": str(self.billing.root),
            "transaction_items": str(self.billing.path("transaction_items.csv")),
        }


class LocalFsArtifactStore:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir

    def put_bytes(self, key: str, data: bytes, content_type: str = "") -> None:
        _ = content_type
        key = str(key).lstrip("/")
        p = self.base_dir / key
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        key = str(key).lstrip("/")
        p = self.base_dir / key
        return p.read_bytes()

    def exists(self, key: str) -> bool:
        key = str(key).lstrip("/")
        return (self.base_dir / key).exists()

    def list_keys(self, prefix: str = "") -> List[str]:
        prefix = str(prefix).lstrip("/")
        root = (self.base_dir / prefix) if prefix else self.base_dir
        if not root.exists():
            return []
        out: List[str] = []
        for fp in root.rglob("*"):
            if fp.is_dir():
                continue
            out.append(str(fp.relative_to(self.base_dir)).replace("\\", "/"))
        return sorted(out)

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "base_dir": str(self.base_dir),
        }


class LocalPythonExecutionBackend:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root

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
        use_repo = repo_root
        use_module_path = module_path or (use_repo / "modules" / step.module_id)
        outputs_dir.mkdir(parents=True, exist_ok=True)

        # Apply module secrets from secretstore, unless explicitly provided.
        if env is None:
            store = load_secretstore(use_repo)
            env = env_for_module(store, step.module_id)

        result = execute_module_runner(use_module_path, step.inputs, outputs_dir, env=env)

        now = utcnow_iso()
        run = StepRunRecord(
            module_run_id=str(result.get("module_run_id", "") or ""),
            tenant_id=workorder.tenant_id,
            work_order_id=workorder.work_order_id,
            step_id=step.step_id,
            module_id=step.module_id,
            status=str(result.get("status", "") or ""),
            created_at=now,
            started_at=str(result.get("started_at", "") or ""),
            ended_at=str(result.get("ended_at", "") or ""),
            reason_code=str(result.get("reason_code", "") or ""),
            output_ref=str(outputs_dir),
            report_path=str(result.get("report_path", "") or ""),
            metadata={"step_id": step.step_id},
        )

        return run, [], {"runner_output": result}

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "repo_root": str(self.repo_root),
        }


class GitHubReleasesArtifactPublisher:
    """Placeholder GitHub Releases publisher.

    This factory produces an adapter instance for wiring and configuration visibility.
    Actual publishing is performed by the dedicated scripts/publish_*.py workflows.
    """

    def publish_deliverable(
        self,
        *,
        repo_root: Path,
        record: DeliverableArtifactRecord,
        local_path: Path,
        content_type: str = "application/zip",
    ) -> DeliverableArtifactRecord:
        _ = (repo_root, local_path, content_type)
        return record

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__}


class NoopArtifactPublisher:
    def publish_deliverable(
        self,
        *,
        repo_root: Path,
        record: DeliverableArtifactRecord,
        local_path: Path,
        content_type: str = "application/zip",
    ) -> DeliverableArtifactRecord:
        _ = (repo_root, local_path, content_type)
        return record

    def describe(self) -> Dict[str, Any]:
        return {"class": self.__class__.__name__}


def build_infra(
    *,
    repo_root: Path,
    profile: RuntimeProfile,
    billing_state_dir: Optional[Path] = None,
    runtime_dir: Optional[Path] = None,
) -> InfraBundle:
    """Build concrete adapter instances from a runtime profile."""

    repo_root = repo_root.resolve()
    billing_state_dir = (billing_state_dir or (repo_root / ".billing-state")).resolve()
    runtime_dir = (runtime_dir or (repo_root / "runtime")).resolve()

    # Directory precedence (highest to lowest):
    #   1) Explicit build_infra(...) arguments (CLI flags)
    #   2) Runtime profile adapter settings
    #   3) Repo defaults
    #
    # This is critical for CI/E2E workflows that pass --billing-state-dir/--runtime-dir.
    if billing_state_dir is None:
        bs_override = str(profile.adapters["run_state_store"].settings.get("billing_state_dir", "") or "").strip()
        if bs_override:
            billing_state_dir = Path(bs_override).expanduser().resolve()

    if runtime_dir is None:
        rt_override = str(profile.adapters["artifact_store"].settings.get("runtime_dir", "") or "").strip()
        if rt_override:
            runtime_dir = Path(rt_override).expanduser().resolve()

    # 1) Registry
    reg_kind = profile.adapters["registry"].kind
    if reg_kind == "repo_csv":
        from .adapters.registry_repo import RepoModuleRegistry

        registry = RepoModuleRegistry(repo_root)
    elif reg_kind == "db_postgres":
        dsn = str(profile.adapters["registry"].settings.get("dsn", "") or "").strip()
        registry = PostgresModuleRegistry(dsn)
    else:
        raise ValidationError(f"unknown registry adapter kind: {reg_kind!r}")

    # 2) RunStateStore
    rs_kind = profile.adapters["run_state_store"].kind
    if rs_kind == "billing_state_csv":
        from .adapters.runstate_csv import CsvRunStateStore

        run_state_store = CsvRunStateStore(billing_state_dir)
    elif rs_kind == "db_postgres":
        dsn = str(profile.adapters["run_state_store"].settings.get("dsn", "") or "").strip()
        run_state_store = PostgresRunStateStore(dsn)
    else:
        raise ValidationError(f"unknown run_state_store adapter kind: {rs_kind!r}")

    # 3) LedgerWriter
    lw_kind = profile.adapters["ledger_writer"].kind
    if lw_kind == "billing_state_csv":
        from .adapters.ledger_csv import CsvLedgerWriter

        ledger_writer = CsvLedgerWriter(billing_state_dir, repo_root=repo_root)
    elif lw_kind == "db_postgres":
        dsn = str(profile.adapters["ledger_writer"].settings.get("dsn", "") or "").strip()
        ledger_writer = PostgresLedgerWriter(dsn)
    else:
        raise ValidationError(f"unknown ledger_writer adapter kind: {lw_kind!r}")

    # 4) ArtifactStore
    def _build_artifact_store(kind: str, settings: Dict[str, Any]) -> ArtifactStore:
        if kind == "local_fs":
            from .adapters.artifacts_local import LocalArtifactStore

            base_dir = str(settings.get("base_dir", "") or "").strip()
            if base_dir:
                base = Path(base_dir).expanduser().resolve()
            else:
                base = runtime_dir / "object_store"
            return LocalArtifactStore(base)
        if kind == "s3":
            from .adapters.artifacts_s3 import S3ArtifactStore, S3ArtifactStoreSettings

            bucket = str(settings.get("bucket", "") or "").strip()
            prefix = str(settings.get("prefix", "") or "").strip()
            region = str(settings.get("region", "") or "").strip()
            return S3ArtifactStore(settings=S3ArtifactStoreSettings(bucket=bucket, prefix=prefix, region=region))
        if kind == "github_release":
            from .adapters.artifacts_github_release import GitHubReleaseArtifactStore, GitHubReleaseArtifactStoreSettings

            default_tag = str(settings.get("default_tag", "") or "").strip() or "platform-artifacts"
            token_env_var = str(settings.get("token_env_var", "") or "").strip() or "GITHUB_TOKEN"
            return GitHubReleaseArtifactStore(
                repo_root=repo_root,
                settings=GitHubReleaseArtifactStoreSettings(default_tag=default_tag, token_env_var=token_env_var),
            )
        if kind == "multi":
            from .adapters.artifacts_multi import MultiArtifactStore, MultiArtifactStoreSettings

            policy = str(settings.get("policy", "") or "").strip() or "fail_fast"
            stores_cfg = settings.get("stores")
            if not isinstance(stores_cfg, list) or not stores_cfg:
                raise ValidationError("artifact_store multi requires settings.stores list")
            child_stores: List[ArtifactStore] = []
            for s in stores_cfg:
                if not isinstance(s, dict):
                    raise ValidationError("artifact_store multi stores entries must be objects")
                ck = str(s.get("kind", "") or "").strip()
                cs = s.get("settings") or {}
                if not isinstance(cs, dict):
                    cs = {}
                child_stores.append(_build_artifact_store(ck, cs))
            return MultiArtifactStore(child_stores, MultiArtifactStoreSettings(policy=policy))
        raise ValidationError(f"unknown artifact_store adapter kind: {kind!r}")

    as_kind = profile.adapters["artifact_store"].kind
    artifact_store = _build_artifact_store(as_kind, profile.adapters["artifact_store"].settings)

    # 5) ExecutionBackend
    eb_kind = profile.adapters["execution_backend"].kind
    if eb_kind == "local_python":
        from .adapters.exec_local import LocalExecutionBackend

        execution_backend = LocalExecutionBackend(repo_root=repo_root, registry=registry, run_state=run_state_store)
    elif eb_kind == "external_engine":
        endpoint = str(profile.adapters["execution_backend"].settings.get("endpoint", "") or "").strip()
        timeout = int(profile.adapters["execution_backend"].settings.get("timeout_seconds", 900) or 900)
        execution_backend = ExternalEngineExecutionBackend(endpoint=endpoint, timeout_seconds=timeout)
    else:
        raise ValidationError(f"unknown execution_backend adapter kind: {eb_kind!r}")

    # 6) ArtifactPublisher
    ap_kind = profile.adapters["artifact_publisher"].kind
    if ap_kind == "github_releases":
        artifact_publisher = GitHubReleasesArtifactPublisher()
    elif ap_kind == "cloud_storage":
        target = str(profile.adapters["artifact_publisher"].settings.get("target", "") or "").strip()
        bucket = str(profile.adapters["artifact_publisher"].settings.get("bucket", "") or "").strip()
        prefix = str(profile.adapters["artifact_publisher"].settings.get("prefix", "") or "").strip()
        artifact_publisher = CloudStorageArtifactPublisher(target=target, bucket=bucket, prefix=prefix)
    elif ap_kind == "noop":
        artifact_publisher = NoopArtifactPublisher()
    else:
        raise ValidationError(f"unknown artifact_publisher adapter kind: {ap_kind!r}")


    # 7) Optional TenantCredentialsStore
    tcs = None
    if "tenant_credentials_store" in profile.adapters:
        tcs_kind = profile.adapters["tenant_credentials_store"].kind
        if tcs_kind == "csv_dev":
            from .adapters.tenant_credentials_csv import TenantCredentialsStoreCsv

            tenants_root_setting = str(profile.adapters["tenant_credentials_store"].settings.get("tenants_root", "tenants") or "tenants").strip()
            tenants_root = (repo_root / tenants_root_setting).resolve()
            tcs = TenantCredentialsStoreCsv(repo_root=repo_root, tenants_root=tenants_root)
        elif tcs_kind == "db_postgres":
            tcs = TenantCredentialsStorePlaceholder(kind=tcs_kind)
        else:
            raise ValidationError(f"unknown tenant_credentials_store adapter kind: {tcs_kind!r}")
    return InfraBundle(
        profile=profile,
        registry=registry,
        run_state_store=run_state_store,
        ledger_writer=ledger_writer,
        artifact_store=artifact_store,
        execution_backend=execution_backend,
        artifact_publisher=artifact_publisher,
        tenant_credentials_store=tcs,
    )
