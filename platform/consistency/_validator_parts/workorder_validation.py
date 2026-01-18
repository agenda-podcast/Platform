# Generated. Do not edit by hand.
CHUNK = r'''\
    enum = rule.enum_values()
    if enum is not None and value not in enum:
        _fail(f"{ctx}: value not in enum {enum}")

    # String constraints
    if isinstance(value, str):
        mn = _as_int(rule.min_length)
        mx = _as_int(rule.max_length)
        if mn is not None and len(value) < mn:
            _fail(f"{ctx}: string length {len(value)} < min_length {mn}")
        if mx is not None and len(value) > mx:
            _fail(f"{ctx}: string length {len(value)} > max_length {mx}")
        if rule.regex:
            try:
                if re.search(rule.regex, value) is None:
                    _fail(f"{ctx}: string does not match regex {rule.regex!r}")
            except re.error:
                # Bad regex in servicing table should not silently pass.
                _fail(f"{ctx}: invalid regex in rules table")

    # Numeric constraints
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        mnv = _as_float(rule.min_value)
        mxv = _as_float(rule.max_value)
        if mnv is not None and float(value) < mnv:
            _fail(f"{ctx}: value {value} < min_value {mnv}")
        if mxv is not None and float(value) > mxv:
            _fail(f"{ctx}: value {value} > max_value {mxv}")

    # Array constraints
    if isinstance(value, list):
        mni = _as_int(rule.min_items)
        mxi = _as_int(rule.max_items)
        if mni is not None and len(value) < mni:
            _fail(f"{ctx}: array length {len(value)} < min_items {mni}")
        if mxi is not None and len(value) > mxi:
            _fail(f"{ctx}: array length {len(value)} > max_items {mxi}")
        it = (rule.item_type or "").strip()
        if it:
            for i, item in enumerate(value):
                if _is_binding(item):
                    continue
                _validate_scalar_type(it, item, f"{ctx}[{i}]")


def load_rules_table(repo_root: Path) -> Dict[str, List[RuleRow]]:
    path = repo_root / "maintenance-state" / "module_contract_rules.csv"
    if not path.exists():
        _fail(f"Missing servicing table: {path}")
    rows = read_csv(path)
    out: Dict[str, List[RuleRow]] = {}
    for r in rows:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        rr = RuleRow(
            module_id=mid,
            module_hash=str(r.get("module_hash", "") or ""),
            io=str(r.get("io", "") or ""),
            port_scope=str(r.get("port_scope", "") or ""),
            field_name=str(r.get("field_name", "") or ""),
            field_id=str(r.get("field_id", "") or ""),
            type=str(r.get("type", "") or ""),
            item_type=str(r.get("item_type", "") or ""),
            format=str(r.get("format", "") or ""),
            required=str(r.get("required", "") or "").strip().lower() == "true",
            default_json=str(r.get("default_json", "") or ""),
            min_value=str(r.get("min_value", "") or ""),
            max_value=str(r.get("max_value", "") or ""),
            min_length=str(r.get("min_length", "") or ""),
            max_length=str(r.get("max_length", "") or ""),
            min_items=str(r.get("min_items", "") or ""),
            max_items=str(r.get("max_items", "") or ""),
            regex=str(r.get("regex", "") or ""),
            enum_json=str(r.get("enum_json", "") or ""),
            description=str(r.get("description", "") or ""),
            examples_json=str(r.get("examples_json", "") or ""),
            path=str(r.get("path", "") or ""),
            content_schema_json=str(r.get("content_schema_json", "") or ""),
            binding_json=str(r.get("binding_json", "") or ""),
            rule_json=str(r.get("rule_json", "") or ""),
        )
        out.setdefault(mid, []).append(rr)
    return out


def _index_module_rules(rules: List[RuleRow]) -> Tuple[Dict[str, RuleRow], Dict[str, RuleRow], Set[str]]:
    """Return (tenant_inputs, all_inputs, exposed_outputs).

    exposed_outputs contains both output IDs and output relative paths for tenant-visible outputs.
    This supports two binding styles:
      - file binding: from_file references an exposed output path
      - output binding: output_id references an exposed output id
    """
    inputs: Dict[str, RuleRow] = {}
    tenant_inputs: Dict[str, RuleRow] = {}
    exposed_outputs: Set[str] = set()
    for r in rules:
        if r.io.upper() == "INPUT":
            if r.field_id:
                inputs[r.field_id] = r
                if r.is_tenant_visible:
                    tenant_inputs[r.field_id] = r
        elif r.io.upper() == "OUTPUT":
            if not r.is_tenant_visible:
                continue
            if r.field_id:
                exposed_outputs.add(str(r.field_id).strip())
            if r.path:
                exposed_outputs.add(r.path.lstrip("/").strip())
    return tenant_inputs, inputs, exposed_outputs
def validate_workorder(repo_root: Path, workorder_path: Path, module_rules_by_id: Dict[str, List[RuleRow]]) -> None:
    # Strict validation for enabled workorders.
    # Drafts (enabled=false) are allowed, but preflight may return warnings.
    data = _read_yaml(workorder_path)
    if not isinstance(data, dict):
        _fail(f"Invalid YAML: {workorder_path}")
    enabled = bool(data.get("enabled", True))
    # Always run preflight; it will raise only when enabled=true.
    _ = validate_workorder_preflight(repo_root, workorder_path, module_rules_by_id)
    return


def validate_workorder_preflight(repo_root: Path, workorder_path: Path, module_rules_by_id: Dict[str, List[RuleRow]]) -> Dict[str, Any]:
    # Validate + defaults/enrichment only (no module execution).
    data = _read_yaml(workorder_path)
    if not isinstance(data, dict):
        _fail(f"Invalid YAML: {workorder_path}")

    warnings: List[str] = []
    enabled = bool(data.get("enabled", True))
    artifacts_requested = bool(data.get("artifacts_requested", False))

    tid = canon_tenant_id(data.get("tenant_id") or workorder_path.parent.parent.name)
    if tid:
        validate_id("tenant_id", tid, "tenant_id")

    steps = data.get("steps")
    if not isinstance(steps, list) or not steps:
        if enabled:
            _fail(f"{workorder_path}: workorder must define non-empty steps list")
        warnings.append(f"{workorder_path}: draft warning: missing or empty steps list")
        return {"enabled": False, "path": str(workorder_path), "warnings": warnings}

    # Collect step IDs and module IDs for enabled steps.
    step_ids: Set[str] = set()
    step_module: Dict[str, str] = {}
    step_cfg_by_id: Dict[str, Dict[str, Any]] = {}
    step_kind_by_id: Dict[str, str] = {}
    ordered_enabled_steps: List[str] = []

    for s in steps:
        if not isinstance(s, dict):
            continue
        step_enabled = bool(s.get("enabled", True))
        if not step_enabled:
            continue

        sid = str(s.get("step_id") or "").strip()
        mid = canon_module_id(s.get("module_id") or "")
        if not sid or not mid:
            continue

        validate_id("step_id", sid, "workorder.step.step_id")
        validate_id("module_id", mid, "workorder.step.module_id")
        if sid in step_ids:
            _fail(f"{workorder_path}: duplicate step_id {sid!r}")
        step_ids.add(sid)
        ordered_enabled_steps.append(sid)
        step_module[sid] = mid
        step_cfg_by_id[sid] = dict(s)

        sk = str(s.get("kind") or "").strip()
        if not sk:
            if enabled:
                _fail(f"{workorder_path}: step {sid!r} missing required field 'kind' (allowed: {list(MODULE_KIND_VALUES)})")
            warnings.append(f"{workorder_path}: draft warning: step {sid!r} missing required field 'kind'")
            continue
        if not is_valid_module_kind(sk):
            if enabled:
                _fail(f"{workorder_path}: step {sid!r} has invalid kind={sk!r} (allowed: {list(MODULE_KIND_VALUES)})")
            warnings.append(f"{workorder_path}: draft warning: step {sid!r} invalid kind={sk!r}")
            continue
        step_kind_by_id[sid] = sk

    # Activation gating (no injection; draft allowed)
    # Rules:
    #  - artifacts_requested=true => packaging + delivery mandatory
    #  - packaging present => delivery mandatory
    #  - delivery present => packaging required and earlier
    #
    # Enabled workorders: failures are blocking.
    # Draft workorders: surface warnings only.
    packaging_steps = [sid for sid in ordered_enabled_steps if step_kind_by_id.get(sid) == "packaging"]
    delivery_steps = [sid for sid in ordered_enabled_steps if step_kind_by_id.get(sid) == "delivery"]

    def _gate_fail_or_warn(msg: str) -> None:
        if enabled:
            _fail(f"{workorder_path}: {msg}")
        warnings.append(f"{workorder_path}: draft warning: {msg}")

    def _step_fail_or_warn(msg: str) -> None:
        """Drafts (enabled=false) must not be blocked by validator.

        We still surface actionable warnings so tenants can iterate safely.
        """
        if enabled:
            _fail(f"{workorder_path}: {msg}")
        warnings.append(f"{workorder_path}: draft warning: {msg}")

    if artifacts_requested:
        if not packaging_steps:
            _gate_fail_or_warn("missing packaging step")
        if not delivery_steps:
            _gate_fail_or_warn("missing delivery step")
        if packaging_steps and delivery_steps:
            first_pack_idx = ordered_enabled_steps.index(packaging_steps[0])
            first_del_idx = ordered_enabled_steps.index(delivery_steps[0])
            if first_del_idx < first_pack_idx:
                _gate_fail_or_warn("wrong order (delivery before packaging)")

    if packaging_steps and not delivery_steps:
        _gate_fail_or_warn("missing delivery step")

    if delivery_steps:
        if not packaging_steps:
            _gate_fail_or_warn("missing packaging step")
        else:
            first_pack_idx = ordered_enabled_steps.index(packaging_steps[0])
            for dsid in delivery_steps:
                didx = ordered_enabled_steps.index(dsid)
                if didx < first_pack_idx:
                    _gate_fail_or_warn("wrong order (delivery before packaging)")
                    break

        # Rule 4: Email delivery requires deterministic size threshold declaration on packaging step.
        for dsid in delivery_steps:
            cfg = step_cfg_by_id.get(dsid) or {}
            method = ""
            if isinstance(cfg.get("delivery"), dict):
                method = str((cfg.get("delivery") or {}).get("method") or "").strip().lower()
            if not method:
                method = str(cfg.get("delivery_method") or "").strip().lower()
            if method != "email":
                continue

            # nearest packaging step before this delivery step
            didx = ordered_enabled_steps.index(dsid)
            prev_pack = None
            for sid in reversed(ordered_enabled_steps[:didx]):
                if step_kind_by_id.get(sid) == "packaging":
                    prev_pack = sid
                    break
            if not prev_pack:
                _fail(f"{workorder_path}: delivery email step {dsid!r} requires a prior packaging step")

            pcfg = step_cfg_by_id.get(prev_pack) or {}
            max_bytes = None
            if isinstance(pcfg.get("packaging"), dict):
                max_bytes = (pcfg.get("packaging") or {}).get("max_bytes")
            if max_bytes is None:
                max_bytes = pcfg.get("max_bytes")
            if max_bytes is None:
                max_bytes = pcfg.get("artifact_max_bytes")

            mb = _as_int(max_bytes)
            if mb is None:
                _fail(
                    f"{workorder_path}: delivery email step {dsid!r} requires packaging step {prev_pack!r} to declare max_bytes < {EMAIL_ATTACHMENT_THRESHOLD_BYTES}"
                )
            if mb >= EMAIL_ATTACHMENT_THRESHOLD_BYTES:
                _fail(
                    f"{workorder_path}: delivery email step {dsid!r} requires packaging step {prev_pack!r} max_bytes < {EMAIL_ATTACHMENT_THRESHOLD_BYTES} (got {mb})"
                )

    # Validate deliverables contract for each module used.
    module_deliverables: Dict[str, Set[str]] = {}
    module_contracts: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for mid in sorted(set(step_module.values())):
        rules = module_rules_by_id.get(mid)
        if not rules:
            _step_fail_or_warn(f"missing module rules for module_id {mid!r} (run Maintenance)")
            # Without rules, we cannot safely validate constraints; skip the rest for drafts.
            continue

        module_kind = _load_module_kind(repo_root, mid)
        contract = _load_module_deliverables_contract(repo_root, mid)
        module_contracts[mid] = contract
        module_deliverables[mid] = set(contract.keys())

        for sid, smid in step_module.items():
            if smid != mid:
                continue
            sk = step_kind_by_id.get(sid)
            if not sk:
                continue
            if sk != module_kind:
                _step_fail_or_warn(f"step {sid!r} kind {sk!r} does not match module {mid!r} kind {module_kind!r}")

        # Validate deliverables.limited_inputs keys against ports rules (must be limited_port).
        _tenant_inputs, all_inputs, _exposed = _index_module_rules(rules)
        for did, dcfg in contract.items():
            lim = (dcfg.get("limited_inputs") or {})
            if not isinstance(lim, dict):
                continue
            for k in lim.keys():
                key = str(k)
                if key not in all_inputs:
                    _step_fail_or_warn(f"module {mid!r} deliverable {did!r} limited_input {key!r} not declared in ports")
                    continue
                rr = all_inputs[key]
                if rr.is_tenant_visible:
                    _step_fail_or_warn(f"module {mid!r} deliverable {did!r} limited_input {key!r} must be limited_port")

    # Validate requested deliverables (explicit + legacy mapping).
    per_step_requested: Dict[str, List[str]] = {}
    per_step_source: Dict[str, str] = {}
    for sid, mid in step_module.items():
        cfg = step_cfg_by_id.get(sid) or {}
        req, src = _normalize_requested_deliverables_for_preflight(repo_root, mid, cfg)
        per_step_requested[sid] = req
        per_step_source[sid] = src
        if req:
            allowed = module_deliverables.get(mid) or set()
            for did in req:
                if did not in allowed:
                    _step_fail_or_warn(f"step {sid!r} deliverable {did!r} not declared by module {mid!r}")

    # Precompute exposed outputs per step (from servicing table) for binding validation.
    step_outputs: Dict[str, Set[str]] = {}
    for sid, mid in step_module.items():
        rules = module_rules_by_id.get(mid) or []
        _, _, exposed = _index_module_rules(rules)
        step_outputs[sid] = exposed

    enriched_steps: List[Dict[str, Any]] = []

    # Validate and enrich step inputs (defaults + deliverables-limited_inputs), then validate bindings.
    for sid, mid in step_module.items():
        rules = module_rules_by_id.get(mid) or []
        tenant_inputs, all_inputs, _ = _index_module_rules(rules)

        cfg = step_cfg_by_id.get(sid) or {}
        inputs_obj = cfg.get("inputs") or {}
        if not isinstance(inputs_obj, dict):
            _step_fail_or_warn(f"step {sid!r} inputs must be an object")
            inputs_obj = {}

        # Reject tenant-provided limited_port inputs and unknown inputs.
        for k in inputs_obj.keys():
            fid = str(k)
            if fid not in all_inputs:
                _step_fail_or_warn(f"step {sid!r} module {mid} has unknown input {fid!r}")
                continue
            rr = all_inputs[fid]
            if not rr.is_tenant_visible:
                _step_fail_or_warn(f"step {sid!r} input {fid!r} is limited_port and must not be set by tenant")

        # Apply defaults (tenant-visible inputs).
        effective: Dict[str, Any] = dict(inputs_obj)
        defaults_applied: List[str] = []
        for fid, rr in all_inputs.items():
            if fid in effective:
                continue
            has_def, dv = _parse_default(rr, f"{workorder_path}: step {sid!r} input {fid!r}")
            if has_def:
                _validate_constraints(rr, dv, f"{workorder_path}: step {sid!r} default {fid!r}")
                effective[fid] = dv
                defaults_applied.append(fid)

        # Apply deliverables-driven platform-only inputs (limited_port)
        req = per_step_requested.get(sid, []) or []
        contract = module_contracts.get(mid) or {}
        applied_limited = _union_limited_inputs(contract, req)
        for k, v in (applied_limited or {}).items():
            if k not in all_inputs:
                _step_fail_or_warn(f"step {sid!r} deliverables set unknown limited_input {k!r} for module {mid!r}")
                continue
            rr = all_inputs[str(k)]
            if rr.is_tenant_visible:

'''

def get_part() -> str:
    return CHUNK
