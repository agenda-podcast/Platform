# Generated. Do not edit by hand.
CHUNK = r'''\
    temp: Set[str] = set()
    perm: Set[str] = set()

    def visit(n: str) -> None:
        if n in perm:
            return
        if n in temp:
            raise ValueError(f"Cycle in dependencies at {n}")
        temp.add(n)
        for d in sorted(edges.get(n, set())):
            visit(d)
        temp.remove(n)
        perm.add(n)
        ordered.append(n)

    for n in nodes:
        visit(n)
    return ordered


def _load_binding_value(step_outputs_dir: Path, binding: Dict[str, Any]) -> Any:
    """Load and transform a value from an upstream step output file."""
    rel_file = str(binding.get("from_file") or binding.get("file") or "").strip()
    if not rel_file:
        raise FileNotFoundError("binding.from_file is required")
    selector = str(binding.get("selector") or "").strip().lower() or "text"
    take = binding.get("take")
    take_n: Optional[int] = None
    try:
        if take is not None:
            take_n = int(take)
    except Exception:
        take_n = None

    fp = step_outputs_dir / rel_file
    if not fp.exists() or not fp.is_file():
        raise FileNotFoundError(str(fp))

    if selector == "text":
        return fp.read_text(encoding="utf-8", errors="replace")

    if selector == "lines":
        lines = [ln.strip() for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines()]
        lines = [ln for ln in lines if ln]
        if take_n is not None:
            lines = lines[: max(0, take_n)]
        return lines

    if selector == "json":
        data = json.loads(fp.read_text(encoding="utf-8", errors="replace") or "null")
        jp = str(binding.get("json_path") or "").strip()
        return _json_path_get(data, jp) if jp else data

    if selector == "jsonl_first":
        first = None
        for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines():
            if ln.strip():
                first = ln
                break
        if first is None:
            raise ValueError("jsonl_first: file is empty")
        data = json.loads(first)
        jp = str(binding.get("json_path") or "").strip()
        return _json_path_get(data, jp) if jp else data

    if selector == "jsonl":
        out: List[Any] = []
        for ln in fp.read_text(encoding="utf-8", errors="replace").splitlines():
            s = ln.strip()
            if not s:
                continue
            out.append(json.loads(s))
            if take_n is not None and len(out) >= take_n:
                break
        return out

    raise ValueError(f"Unsupported binding selector: {selector}")


def _extract_step_edges(steps: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """Infer step dependencies from input bindings (from_step)."""
    edges: Dict[str, Set[str]] = {}
    known = {str(s.get("step_id") or "").strip() for s in steps if str(s.get("step_id") or "").strip()}
    for s in steps:
        sid = str(s.get("step_id") or "").strip()
        if not sid:
            continue
        deps = {d for d in _collect_bind_deps(s.get("inputs") or {}) if d in known}
        # Optional explicit dependencies
        explicit = s.get("depends_on") or s.get("needs")
        if isinstance(explicit, list):
            for x in explicit:
                xs = str(x).strip()
                if xs and xs in known:
                    deps.add(xs)
        edges[sid] = deps
    return edges


def _resolve_inputs(
    inputs_spec: Any,
    step_outputs: Dict[str, Path],
    allowed_outputs: Dict[str, Set[str]],
    run_state: Any,
    tenant_id: str,
    work_order_id: str,
) -> Any:
    """Resolve bindings within an inputs spec.

    Supported binding forms:
      - file selector binding: {from_step, from_file, selector, ...}
      - output binding (Option A): {from_step, output_id, ...} returning a resolved OutputRecord dict

    Rules:
      - bindings may appear at any depth (inside dicts/lists)
      - for file bindings, from_file must be exposed by upstream step (tenant-visible outputs)
      - for output bindings, output_id must exist in run_state and its output path must be exposed
    """
    if _is_binding(inputs_spec):
        src = inputs_spec
        if isinstance(inputs_spec.get("from"), dict):
            src = inputs_spec.get("from") or {}

        from_step = str(src.get("from_step") or src.get("step_id") or "").strip()
        if not from_step:
            raise ValueError("binding.from_step is required")
        if from_step not in step_outputs:
            raise FileNotFoundError(f"Upstream step outputs not found: {from_step}")

        output_id = str(src.get("output_id") or src.get("from_output_id") or "").strip()
        if output_id:
            rec = run_state.get_output(tenant_id, work_order_id, from_step, output_id)
            allowed = allowed_outputs.get(from_step) or set()
            if allowed and rec.path and rec.path not in allowed:
                raise PermissionError(
                    f"binding.output_id '{output_id}' is not exposed by upstream step '{from_step}' (allowed: {sorted(allowed)})"
                )
            out = asdict(rec)
            if "as_path" in src:
                out["as_path"] = src.get("as_path")
            elif "as" in src:
                out["as_path"] = src.get("as")
            return out

        from_file = str(src.get("from_file") or "").lstrip("/").strip()
        if not from_file:
            raise ValueError("binding.from_file or binding.output_id is required")

        allowed = allowed_outputs.get(from_step) or set()
        if allowed and from_file not in allowed:
            raise PermissionError(
                f"binding.from_file '{from_file}' is not exposed by upstream step '{from_step}' (allowed: {sorted(allowed)})"
            )
        return _load_binding_value(step_outputs[from_step], src)

    if isinstance(inputs_spec, dict):
        return {k: _resolve_inputs(v, step_outputs, allowed_outputs, run_state, tenant_id, work_order_id) for k, v in inputs_spec.items()}
    if isinstance(inputs_spec, list):
        return [_resolve_inputs(v, step_outputs, allowed_outputs, run_state, tenant_id, work_order_id) for v in inputs_spec]
    return inputs_spec
def _build_execution_plan(workorder: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize a workorder into an ordered execution plan.

    Returns:
      - plan: list of dicts with keys: step_id, module_id, cfg
    """
    steps = workorder.get("steps")
    if not (isinstance(steps, list) and steps):
        raise ValueError("Workorder must define non-empty 'steps' list (legacy modules-only workorders are not supported)")

    plan_steps: List[Dict[str, Any]] = []
    for s in steps:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("step_id") or "").strip()
        mid = canon_module_id(s.get("module_id") or "")
        if not sid or not mid:
            continue
        validate_id("step_id", sid, "workorder.step.step_id")
        plan_steps.append({"step_id": sid, "module_id": mid, "cfg": s})

    edges = _extract_step_edges([p["cfg"] for p in plan_steps])
    # edges keys refer to step_id; ensure node list order is stable as in YAML
    nodes = [p["step_id"] for p in plan_steps]
    ordered_sids = _toposort_nodes(nodes, edges)
    by_id = {p["step_id"]: p for p in plan_steps}
    return [by_id[sid] for sid in ordered_sids if sid in by_id]


def _load_tenant_relationships(repo_root: Path) -> Set[Tuple[str, str]]:
    rows = read_csv(repo_root / "maintenance-state" / "tenant_relationships.csv")
    out=set()
    for r in rows:
        s = canon_tenant_id(r.get("source_tenant_id",""))
        t = canon_tenant_id(r.get("target_tenant_id",""))
        if s and t:
            out.add((s,t))
    return out


def _load_module_artifacts_policy(repo_root: Path) -> Dict[str, bool]:
    rows = read_csv(repo_root / "maintenance-state" / "module_artifacts_policy.csv")
    out: Dict[str, bool] = {}
    for r in rows:
        mid = canon_module_id(r.get("module_id",""))
        if not mid:
            continue
        out[mid] = str(r.get("platform_artifacts_enabled","")).strip().lower() == "true"
    return out


def _new_id(id_type: str, used: Set[str]) -> str:
    return generate_unique_id(id_type, used)


@dataclass(frozen=True)
class OrchestratorContext:
    tenant_id: str
    work_order_id: str
    run_id: str
    runtime_profile_name: str


def run_orchestrator(repo_root: Path, billing_state_dir: Path, runtime_dir: Path, enable_github_releases: bool = False, infra: InfraBundle | None = None) -> None:
    if infra is None:
        from ..infra.config import load_runtime_profile
        from ..infra.factory import build_infra

        profile = load_runtime_profile(repo_root, cli_path="")
        infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    registry = infra.registry
    run_state = infra.run_state_store
    ledger = infra.ledger_writer
    runtime_profile_name = str(infra.profile.profile_name or "").strip() or "default"

    # Environment reads are centralized at the orchestration entrypoint.
    dev_scan_workorders = (str(os.environ.get('PLATFORM_DEV_SCAN_WORKORDERS', '') or '').strip() == '1')
    secretstore_passphrase_present = bool(str(os.environ.get('SECRETSTORE_PASSPHRASE', '') or '').strip())
    github_token_present = bool(os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN'))

    # Preflight validator (no execution). Enabled workorders must pass before any billing or execution.
    from platform.consistency.validator import ConsistencyValidationError, load_rules_table, validate_workorder_preflight

    module_rules_by_id = load_rules_table(repo_root)
    run_since = utcnow_iso()
    from platform.config.load_platform_config import load_platform_config
    platform_cfg = load_platform_config(repo_root)
    cache_ttl_days_by_place_type = _parse_ttl_days_by_place_type(platform_cfg)
    cache_ttl_days = cache_ttl_days_by_place_type.get(('cache','module_run'))
    if cache_ttl_days is None:
        raise ValueError("Missing cache TTL rule for 'cache:module_run' in platform/config/platform_config.yml")

    reason_idx = _load_reason_index(repo_root)
    tenant_rel = _load_tenant_relationships(repo_root)
    prices = _load_module_prices(repo_root)
    artifacts_policy = _load_module_artifacts_policy(repo_root)
    module_names = _load_module_display_names(registry)

    billing = BillingState(billing_state_dir)
    billing_state_dir.mkdir(parents=True, exist_ok=True)

    # Orchestrator needs full state including mapping tables
    billing.validate_minimal(
        required_files=[
            "tenants_credits.csv",
            "transactions.csv",
            "transaction_items.csv",
            "promotion_redemptions.csv",
            "cache_index.csv",
                                    "github_releases_map.csv",
            "github_assets_map.csv",
        ]
    )

    tenants_credits = dedupe_tenants_credits(billing.load_table("tenants_credits.csv"))
    transactions = billing.load_table("transactions.csv")
    transaction_items = billing.load_table("transaction_items.csv")
    cache_index = billing.load_table("cache_index.csv")
    promo_redemptions = billing.load_table("promotion_redemptions.csv")
    rel_map = billing.load_table("github_releases_map.csv")
    asset_map = billing.load_table("github_assets_map.csv")

    used_tx: Set[str] = {id_key(r.get("transaction_id")) for r in transactions if id_key(r.get("transaction_id"))}
    used_ti: Set[str] = {id_key(r.get("transaction_item_id")) for r in transaction_items if id_key(r.get("transaction_item_id"))}
    used_mr: Set[str] = set()
    used_rel: Set[str] = {id_key(r.get("release_id")) for r in rel_map if id_key(r.get("release_id"))}
    used_asset: Set[str] = {id_key(r.get("asset_id")) for r in asset_map if id_key(r.get("asset_id"))}

    runtime_dir.mkdir(parents=True, exist_ok=True)
    ensure_dir(runtime_dir)

    # Local module output cache (persisted across workflow runs via actions/cache).
    cache_root = runtime_dir / "cache_outputs"
    ensure_dir(cache_root)

    queue_source, workorders = _load_workorders_queue(repo_root)

    # Module deliverables contracts cached per run
    deliverables_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # Load encrypted secretstore once per run (if configured).
    store = load_secretstore(repo_root)
    if store.version <= 0:
        if secretstore_passphrase_present:
            print('[secretstore] WARNING: passphrase provided but store version is 0 (decrypt/parse may have failed).')
        else:
            print('[secretstore] INFO: SECRETSTORE_PASSPHRASE not provided; proceeding without injected secrets.')
    else:
        mods = store.raw.get('modules') or {}
        if isinstance(mods, dict):
            print(f"[secretstore] loaded version={store.version} modules={sorted(mods.keys())}")


    # Run-scoped summary (useful in CI logs)
    print("\nORCHESTRATOR RUN-SCOPED SUMMARY")
    print(f"billing_state_dir: {billing_state_dir}")
    print(f"runtime_dir:       {runtime_dir}")
    print(f"tenants_dir:       {repo_root / 'tenants'}")
    print(f"since:             {run_since}")
    print("")
    print(f"queue_source:       {queue_source}")
    print(f"Queued workorders:  {len(workorders)}")
    for it in workorders:
        print(f" - {it.get('path')}")
    print("")


    # Auto-enable GitHub Releases when artifacts were purchased.
    # This keeps offline/local runs stable while ensuring "download artifacts" works in GitHub Actions.
    if not enable_github_releases:
        token_present = github_token_present
        if token_present:
            wants_releases = False
            for it in workorders:
                w = dict(it.get("workorder") or {})
                if not bool(w.get("enabled", True)):
                    continue
                _plan = _build_execution_plan(w)
                for step in _plan:
                    mid = canon_module_id(step.get("module_id", ""))
                    cfg = dict(step.get("cfg") or {})
                    if not mid:
                        continue
                    req, _src = _normalize_requested_deliverables(repo_root, registry, mid, cfg, deliverables_cache)
                    if bool(req) and artifacts_policy.get(mid, True):
                        wants_releases = True
                        break
                if wants_releases:
                    break
            if wants_releases:
                enable_github_releases = True

    for item in workorders:
        w = dict(item["workorder"])
        if not bool(w.get("enabled", True)):
            continue

        tenant_id = canon_tenant_id(item["tenant_id"])
        work_order_id = canon_work_order_id(item["work_order_id"])
        # Preflight validation hook: enabled workorders must be valid; drafts (disabled) are allowed to exist.
        workorder_path = repo_root / str(item.get("path") or "")
        try:
            validate_workorder_preflight(repo_root, workorder_path, module_rules_by_id)
        except ConsistencyValidationError as e:
            raise RuntimeError(f"Workorder preflight failed for {workorder_path}: {e}") from e

        created_at = str((w.get("metadata") or {}).get("created_at") or utcnow_iso())
        started_at = utcnow_iso()

        # pricing + execution plan
        plan_type = "steps"
        plan = _build_execution_plan(w)
        print(f"[orchestrator] work_order_id={work_order_id} tenant_id={tenant_id} plan_type={plan_type} steps={[p.get('step_id') for p in plan]}")

        est_total = 0
        per_step_requested_deliverables: Dict[str, List[str]] = {}
        per_step_deliverables_source: Dict[str, str] = {}
        per_step_price_breakdown: Dict[str, Dict[str, int]] = {}
        for step in plan:
            sid = str(step.get("step_id") or '').strip()
            mid = canon_module_id(step.get("module_id", ""))
            cfg = dict(step.get("cfg") or {})
            req_deliverables, del_src = _normalize_requested_deliverables(repo_root, registry, mid, cfg, deliverables_cache)
            breakdown = _price_breakdown_for_step(prices, mid, req_deliverables)
            if sid:
                per_step_requested_deliverables[sid] = req_deliverables
                per_step_deliverables_source[sid] = del_src
                per_step_price_breakdown[sid] = breakdown
            est_total += _sum_prices(breakdown)

        # Record run context in adapterized run-state store (append-only, latest-wins semantics).
        artifacts_requested = False
        for _sid, _dids in per_step_requested_deliverables.items():
            if _dids:
                artifacts_requested = True
                break

        ctx = OrchestratorContext(
            tenant_id=tenant_id,
'''

def get_chunk() -> str:
    return CHUNK
