# Generated. Do not edit by hand.
CHUNK = r'''\
                _step_fail_or_warn(f"step {sid!r} deliverables set tenant-visible input {k!r} (must be limited_port)")
                continue
            _validate_constraints(rr, v, f"{workorder_path}: step {sid!r} deliverables limited_input {k!r}")
            effective[k] = v

        # Required inputs (tenant-visible + limited_port) must be present after enrichment.
        for fid, rr in all_inputs.items():
            if not rr.required:
                continue
            if fid not in effective:
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue
            vv = effective.get(fid)
            if vv is None:
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue
            if isinstance(vv, str) and not vv.strip() and rr.type == "string":
                _step_fail_or_warn(f"step {sid!r} missing required input {fid!r} for module {mid}")
                continue

        # Validate each effective input.
        for fid, val in effective.items():
            rr = all_inputs.get(str(fid))
            if rr is None:
                continue
            ctx = f"{workorder_path}: step {sid!r} input {fid!r}"

            # Bindings may appear nested within lists/dicts; validate each binding reference.
            for b in _iter_bindings(val):
                try:
                    _validate_binding(b, rr, ctx, step_ids, step_outputs)
                except ConsistencyValidationError as e:
                    if enabled:
                        raise
                    warnings.append(f"{workorder_path}: draft warning: {str(e)}")

            # If the value itself is a binding object, we skip scalar constraints (bindings are validated above).
            if _is_binding(val):
                continue
            try:
                _validate_constraints(rr, val, ctx)
            except ConsistencyValidationError as e:
                if enabled:
                    raise
                warnings.append(f"{workorder_path}: draft warning: {str(e)}")

        enriched_steps.append(
            {
                "step_id": sid,
                "module_id": mid,
                "kind": step_kind_by_id.get(sid, ""),
                "requested_deliverables": req,
                "deliverables_source": per_step_source.get(sid, "none"),
                "defaults_applied": defaults_applied,
                "effective_inputs": effective,
            }
        )

    return {
        "enabled": enabled,
        "path": str(workorder_path),
        "tenant_id": tid,
        "artifacts_requested": artifacts_requested,
        "steps": enriched_steps,
        "warnings": warnings,
    }


def integrity_validate(repo_root: Path, work_order_id: str = "", tenant_id: str = "", path: str = "") -> List[Dict[str, Any]]:
    # Returns list of per-workorder results. Raises ConsistencyValidationError on failure.
    rules = load_rules_table(repo_root)

    def _resolve_path_from_index(wid: str, tid: str) -> Path:
        idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
        if not idx_path.exists():
            _fail("maintenance-state/workorders_index.csv missing (run Maintenance)")
        rows = read_csv(idx_path)
        matches = []
        for r in rows:
            enabled = str(r.get('enabled','')).strip().lower() == 'true'
            if not enabled:
                continue
            rwid = str(r.get('work_order_id','')).strip()
            rtid = canon_tenant_id(r.get('tenant_id',''))
            rel = str(r.get('path','')).strip()
            if not rel or not rwid:
                continue
            if wid and rwid != wid:
                continue
            if tid and rtid != canon_tenant_id(tid):
                continue
            matches.append(rel)
        if not matches:
            _fail(f"No enabled workorder found for work_order_id={wid!r} tenant_id={tid!r}")
        if len(matches) > 1:
            _fail(f"Multiple enabled workorders match work_order_id={wid!r}; provide --tenant-id or --path")
        return (repo_root / matches[0]).resolve()

    results: List[Dict[str, Any]] = []

    if path:
        wp = Path(path)
        if not wp.is_absolute():
            wp = (repo_root / wp)
        if not wp.exists():
            _fail(f"workorder path not found: {wp}")
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        return results

    if work_order_id:
        wp = _resolve_path_from_index(str(work_order_id), str(tenant_id))
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        return results

    # Validate all enabled workorders from index
    idx_path = repo_root / 'maintenance-state' / 'workorders_index.csv'
    if not idx_path.exists():
        _fail("maintenance-state/workorders_index.csv missing (run Maintenance)")
    rows = read_csv(idx_path)
    any_validated = False
    for r in rows:
        enabled = str(r.get('enabled','')).strip().lower() == 'true'
        if not enabled:
            continue
        rel = str(r.get('path','')).strip()
        if not rel:
            continue
        wp = repo_root / rel
        if not wp.exists():
            _fail(f"workorders_index references missing file: {rel}")
        results.append(validate_workorder_preflight(repo_root, wp, rules))
        any_validated = True
    if not any_validated:
        return []
    return results


def validate_all_workorders(repo_root: Path) -> None:
    rules = load_rules_table(repo_root)
    idx_path = repo_root / "maintenance-state" / "workorders_index.csv"
    use_scan = (os.environ.get("PLATFORM_DEV_SCAN_WORKORDERS", "").strip() == "1")

    def _emit_warnings(warns: List[str]) -> None:
        for w in warns:
            print(f"DRAFT WARNING: {w}")

    if idx_path.exists() and not use_scan:
        rows = read_csv(idx_path)
        any_seen = False
        for r in rows:
            rel = str(r.get("path", "") or "").strip()
            if not rel:
                continue
            wp = repo_root / rel
            if not wp.exists():
                _fail(f"workorders_index references missing file: {rel}")

            res = validate_workorder_preflight(repo_root, wp, rules)
            any_seen = True
            if not bool(res.get("enabled", False)):
                _emit_warnings(list(res.get("warnings", []) or []))
                continue
        if not any_seen:
            return
        return

    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    any_seen = False
    for td in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not td.is_dir():
            continue
        tenant_id = canon_tenant_id(td.name)
        if tenant_id:
            validate_id("tenant_id", tenant_id, "tenant_id")
        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml")):
            res = validate_workorder_preflight(repo_root, wp, rules)
            any_seen = True
            if not bool(res.get("enabled", False)):
                _emit_warnings(list(res.get("warnings", []) or []))
                continue

    if not any_seen:
        return
        return
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    any_validated = False
    for td in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not td.is_dir():
            continue
        tenant_id = canon_tenant_id(td.name)
        if tenant_id:
            validate_id("tenant_id", tenant_id, "tenant_id")
        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml")):
            validate_workorder(repo_root, wp, rules)
            any_validated = True

    if not any_validated:
        # Not an error: repo may be scaffold-only.
        return
'''

def get_chunk() -> str:
    return CHUNK
