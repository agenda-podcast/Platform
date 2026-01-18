# Generated. Do not edit by hand.
CHUNK = r'''\


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
    # This is critical for CI/offline workflows that pass --billing-state-dir/--runtime-dir.
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

        run_state_store = CsvRunStateStore(runtime_dir / 'runstate')
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

'''

def get_chunk() -> str:
    return CHUNK
