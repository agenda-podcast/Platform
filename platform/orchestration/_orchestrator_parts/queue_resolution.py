"""Orchestrator implementation part (role-based split; kept <= 500 lines)."""

PART = r'''\
            out.append({"tenant_id": tenant_id, "work_order_id": wid, "workorder": w, "path": str(wpath)})
    return out


def _load_workorders_queue(repo_root: Path) -> Tuple[str, List[Dict[str, Any]]]:
    """Load workorders from maintenance-state/workorders_index.csv (canonical queue).

    Fallback: directory scan is allowed only when PLATFORM_DEV_SCAN_WORKORDERS=1 or index missing.
    Returns (queue_source, items).
    """
    # Optional override for verification runners.
    # When set, orchestrator uses this CSV as the canonical queue.
    # This keeps default behavior unchanged while enabling deterministic
    # selection for "verify a specific workorder" flows.
    override = str(os.environ.get('PLATFORM_WORKORDERS_INDEX_PATH', '') or '').strip()
    if override:
        o = Path(override)
        if o.is_absolute():
            idx_path = o
        else:
            idx_path = (repo_root / o).resolve()
    else:
        idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
    # Dev-only override: allow directory scan when explicitly enabled.
    # NOTE: this helper is used both in production code and unit tests; it must not
    # depend on locals from run_orchestrator().
    dev_scan = (str(os.environ.get('PLATFORM_DEV_SCAN_WORKORDERS', '') or '').strip() == '1')
    if (not idx_path.exists()) or dev_scan:
        return ('scan:tenants/*/workorders', _discover_workorders(repo_root))
    rows = read_csv(idx_path)
    out: List[Dict[str, Any]] = []
    for r in rows:
        enabled = str(r.get('enabled','')).strip().lower() == 'true'
        if not enabled:
            continue
        tenant_id = canon_tenant_id(r.get('tenant_id',''))
        work_order_id = canon_work_order_id(r.get('work_order_id',''))
        rel = str(r.get('path','')).strip()
        if not (tenant_id and work_order_id and rel):
            continue
        wpath = repo_root / rel
        if not wpath.exists():
            print(f'[orchestrator] WARNING: workorders_index references missing file: {rel}')
            continue
        w = _repo_yaml(wpath)
        out.append({'tenant_id': tenant_id, 'work_order_id': work_order_id, 'workorder': w, 'path': rel})
    src = str(idx_path)
    try:
        src = str(idx_path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        src = str(idx_path)
    return (src, out)


def _load_reason_index(repo_root: Path) -> ReasonIndex:
    ms = repo_root / "maintenance-state"
    catalog = read_csv(ms / "reason_catalog.csv")
    policy = read_csv(ms / "reason_policy.csv")

    by_key: Dict[Tuple[str, str, str], str] = {}
    for r in catalog:
        scope = str(r.get("scope", "")).strip().upper()
        module_id = str(r.get("module_id", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        code = str(r.get("reason_code", "")).strip()
        if not (scope and slug and code):
            continue
        if scope == "GLOBAL":
            module_id = ""
        by_key[(scope, module_id, slug)] = code

    refundable: Dict[str, bool] = {}
    for r in policy:
        code = str(r.get("reason_code", "")).strip()
        if not code:
            continue
        refundable[code] = str(r.get("refundable", "")).strip().lower() == "true"

    return ReasonIndex(by_key=by_key, refundable=refundable)


def _reason_code(idx: ReasonIndex, scope: str, module_id: str, reason_slug: str) -> str:
    scope_u = scope.upper()
    mid = module_id if scope_u == "MODULE" else ""
    code = idx.by_key.get((scope_u, mid, reason_slug))
    if code:
        return code
    # fallback: global unknown_error if present
    code = idx.by_key.get(("GLOBAL", "", "unknown_error"))
    return code or ""


def _parse_ymd(s: str) -> date:
    s = str(s or "").strip()
    if not s:
        return date(1970, 1, 1)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return date(1970, 1, 1)


def _load_module_prices(repo_root: Path) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Load per-deliverable pricing.

    Schema:
      module_id,deliverable_id,price_credits,effective_from,effective_to,active,notes

    deliverable_id="__run__" is reserved for the per-step execution charge.
    """
    rows = read_csv(repo_root / "platform" / "billing" / "module_prices.csv")
    out: Dict[str, Dict[str, Dict[str, str]]] = {}

    today = datetime.now(timezone.utc).date()

    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        did = str(r.get("deliverable_id", "") or "").strip()
        if not mid or not did:
            continue

        active = str(r.get("active", "") or "").strip().lower() == "true"
        if not active:
            continue

        eff_from = _parse_ymd(r.get("effective_from", ""))
        eff_to_raw = str(r.get("effective_to", "") or "").strip()
        eff_to = _parse_ymd(eff_to_raw) if eff_to_raw else date(2100, 1, 1)
        if not (eff_from <= today <= eff_to):
            continue

        # If multiple rows match, choose the most recent effective_from.
        by_del = out.setdefault(mid, {})
        prev = by_del.get(did)
        if prev is not None:
            prev_from = _parse_ymd(prev.get("effective_from", ""))
            if prev_from >= eff_from:
                continue
        by_del[did] = r

    return out


def _price(prices: Dict[str, Dict[str, Dict[str, str]]], module_id: str, deliverable_id: str) -> int:
    mid = canon_module_id(module_id)
    did = str(deliverable_id or "").strip()
    if not mid or not did:
        return 0
    row = (prices.get(mid) or {}).get(did)
    if not row:
        return 0
    try:
        return int(str(row.get("price_credits", "0")).strip() or "0")
    except Exception:
        return 0


def _price_breakdown_for_step(
    prices: Dict[str, Dict[str, Dict[str, str]]],
    module_id: str,
    requested_deliverables: List[str],
) -> Dict[str, int]:
    """Return per-step pricing breakdown keyed by deliverable_id (including __run__)."""
    out: Dict[str, int] = {"__run__": _price(prices, module_id, "__run__")}
    for did in requested_deliverables or []:
        ds = str(did or "").strip()
        if not ds:
            continue
        out[ds] = _price(prices, module_id, ds)
    return out


def _sum_prices(breakdown: Dict[str, int]) -> int:
    return int(sum(int(v) for v in (breakdown or {}).values()))
def _load_module_display_names(registry: Any) -> Dict[str, str]:
    """Load optional human-readable module names using registry.get_contract (key: module_id)."""
    out: Dict[str, str] = {}
    try:
        mids = list(registry.list_modules())
    except Exception:
        mids = []
    for mid in mids:
        try:
            c = registry.get_contract(mid)
        except Exception:
            continue
        cmid = canon_module_id(c.get("module_id") or mid)
        if not cmid:
            continue
        name = str(c.get("name") or "").strip()
        if name:
            out[cmid] = name
    return out


def _load_module_ports(registry: Any, module_id: str) -> Dict[str, Any]:
    """Load module port definitions from the module contract.

    The platform canonical schema is module.yml: ports.inputs.port / ports.inputs.limited_port
    and ports.outputs.port / ports.outputs.limited_port.

    This function normalizes that schema into the older shape expected by _ports_index:
      - inputs_port: list[dict]
      - inputs_limited_port: list[dict]
      - outputs_port: list[dict]
      - outputs_limited_port: list[dict]

    Backward compatibility: if a contract does not include ports, fall back to legacy
    contract['inputs'] map and contract['outputs'] map.
    """
    mid = canon_module_id(module_id)
    if not mid:
        raise ValueError(f"Invalid module_id for ports: {module_id!r}")

    contract = registry.get_contract(mid)

    in_port = []
    in_limited = []
    out_port = []
    out_limited = []

    ports = contract.get('ports') if isinstance(contract, dict) else None
    if isinstance(ports, dict):
        inputs = ports.get('inputs') if isinstance(ports.get('inputs'), dict) else {}
        outputs = ports.get('outputs') if isinstance(ports.get('outputs'), dict) else {}

        ip = inputs.get('port') or []
        il = inputs.get('limited_port') or []
        op = outputs.get('port') or []
        ol = outputs.get('limited_port') or []

        if isinstance(ip, list):
            in_port = [p for p in ip if isinstance(p, dict)]
        if isinstance(il, list):
            in_limited = [p for p in il if isinstance(p, dict)]
        if isinstance(op, list):
            out_port = [p for p in op if isinstance(p, dict)]
        if isinstance(ol, list):
            out_limited = [p for p in ol if isinstance(p, dict)]

        return {
            'inputs_port': in_port,
            'inputs_limited_port': in_limited,
            'outputs_port': out_port,
            'outputs_limited_port': out_limited,
        }

    # Legacy permissive behavior (older contracts).
    inputs = contract.get('inputs') or {}
    if not isinstance(inputs, dict):
        inputs = {}

    in_port = [v for v in inputs.values() if isinstance(v, dict) and not bool(v.get('is_limited'))]
    in_limited = [v for v in inputs.values() if isinstance(v, dict) and bool(v.get('is_limited'))]

    outputs_map = contract.get('outputs') or {}
    if isinstance(outputs_map, dict):
        for o in outputs_map.values():
            if isinstance(o, dict):
                out_port.append(o)

    return {
        'inputs_port': in_port,
        'inputs_limited_port': in_limited,
        'outputs_port': out_port,
        'outputs_limited_port': out_limited,
    }


def _ports_index(ports: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Set[str]]:
    """Return (tenant_inputs, platform_inputs, tenant_output_paths)."""
    tenant_inputs: Dict[str, Dict[str, Any]] = {}
    platform_inputs: Dict[str, Dict[str, Any]] = {}
    tenant_output_paths: Set[str] = set()

    for p in ports.get("inputs_port") or []:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        if pid:
            tenant_inputs[pid] = p

    for p in ports.get("inputs_limited_port") or []:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        if pid:
            platform_inputs[pid] = p

    for p in ports.get("outputs_port") or []:
        if not isinstance(p, dict):
            continue
        path = str(p.get("path") or "").lstrip("/").strip()
        if path:
            tenant_output_paths.add(path)

    return tenant_inputs, platform_inputs, tenant_output_paths


# ------------------------------------------------------------------
# Deliverables contract (module.yml: deliverables.port)
# ------------------------------------------------------------------

def _load_module_deliverables(registry: Any, module_id: str) -> Dict[str, Dict[str, Any]]:
    """Load deliverables contract using registry.get_contract (no direct filesystem reads)."""
    mid = canon_module_id(module_id)
    if not mid:
        raise ValueError(f"Invalid module_id for deliverables: {module_id!r}")
    contract = registry.get_contract(mid)
    dmap = contract.get("deliverables") or {}
    if not isinstance(dmap, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for did, d in dmap.items():
        if not isinstance(d, dict):
            continue
        out[str(did)] = {"limited_inputs": d.get("limited_inputs") or {}, "output_paths": d.get("output_paths") or []}
    return out


def _normalize_requested_deliverables(
    repo_root: Path,
    registry: Any,
    module_id: str,
    cfg: Dict[str, Any],
    deliverables_cache: Dict[str, Dict[str, Dict[str, Any]]],
) -> Tuple[List[str], str]:
    """Return (requested_deliverables, source).

    Backward-compat mapping:
      - if cfg.deliverables missing and legacy purchase_release_artifacts: true
        map to ['tenant_outputs'] if present; else all deliverables declared by module

    Registry
      Uses registry.get_contract(module_id) to resolve available deliverables.
    """

    _ = repo_root

    if "deliverables" in cfg and cfg.get("deliverables") is not None:
        raw = cfg.get("deliverables")
        if not isinstance(raw, list):
            raise ValueError(f"step.deliverables must be a list for module {module_id}")
        out: List[str] = []
        seen: Set[str] = set()
        for x in raw:
            s = str(x or "").strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out, "explicit"

    if bool(cfg.get("purchase_release_artifacts", False)):
        mid = canon_module_id(module_id)
        if mid not in deliverables_cache:
            try:
                contract = registry.get_contract(mid)
            except Exception:
                contract = {}
            dmap = contract.get("deliverables") or {}
            if not isinstance(dmap, dict):
                dmap = {}
            # Normalize to the shape used by downstream helpers
            norm: Dict[str, Dict[str, Any]] = {}
            for did, d in dmap.items():
                if not isinstance(d, dict):
                    continue
                norm[str(did)] = {
                    "limited_inputs": dict(d.get("limited_inputs") or {}),
                    "output_paths": list(d.get("output_paths") or []),
                }
            deliverables_cache[mid] = norm
        contract2 = deliverables_cache[mid]
        if "tenant_outputs" in contract2:
            return ["tenant_outputs"], "legacy:tenant_outputs"
        if contract2:
            return sorted(contract2.keys()), "legacy:all"
        return [], "legacy:none"

    return [], "none"


def _union_limited_inputs(contract: Dict[str, Dict[str, Any]], requested: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for did in requested:
        d = contract.get(did) or {}
        lim = d.get("limited_inputs") or {}
        if isinstance(lim, dict):
            for k, v in lim.items():
                out[str(k)] = v
    return out


def _deliverable_output_paths(contract: Dict[str, Dict[str, Any]], requested: List[str]) -> List[str]:
    paths: List[str] = []
    seen: Set[str] = set()
    for did in requested:
        d = contract.get(did) or {}
        ops = d.get("output_paths") or []
        if not isinstance(ops, list):
            continue
        for pth in ops:
            ps = str(pth or "").lstrip("/").strip()
            if not ps or ps in seen:
                continue
            seen.add(ps)
            paths.append(ps)
    return paths


def _effective_inputs_hash(inputs: Any) -> str:
    try:
        payload = json.dumps(inputs, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = repr(inputs)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _rel_path_allowed(rel: str, allowed_paths: List[str]) -> bool:
    if not allowed_paths:
        return True
    rel = str(rel).lstrip("/")
    for p in allowed_paths:
        ps = str(p).lstrip("/")
        if not ps:
            continue
        if rel == ps or rel.startswith(ps + "/"):
            return True
    return False


def _toposort_nodes(nodes: List[str], edges: Dict[str, Set[str]]) -> List[str]:
    """Topologically sort nodes based on dependency edges.

    Args:
        nodes: list of node ids to sort (order is preserved where possible)
        edges: mapping node -> set(dependency nodes)

    Returns:
        ordered list where dependencies appear before dependents
    """
    ordered: List[str] = []
'''

def get_part() -> str:
    return PART
