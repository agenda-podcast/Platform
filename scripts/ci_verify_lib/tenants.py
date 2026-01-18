from __future__ import annotations

from .core import *

def _validate_tenants_and_workorders(repo_root: Path) -> None:
    tenants_dir = repo_root / "tenants"
    if not tenants_dir.exists():
        _fail("tenants/ directory missing")

    module_ports_cache: Dict[str, Dict[str, Any]] = {}
    module_deliverables_cache: Dict[str, set[str]] = {}

    def _load_deliverables(mid: str) -> set[str]:
        mid = str(mid).strip()
        if mid in module_deliverables_cache:
            return module_deliverables_cache[mid]
        myml = repo_root / "modules" / mid / "module.yml"
        if not myml.exists():
            _fail(f"Workorder references missing module folder: {mid!r}")
        cfg = _read_yaml(myml)
        deliverables = cfg.get("deliverables") or {}
        port = []
        if isinstance(deliverables, dict):
            port = deliverables.get("port") or []
        if not isinstance(port, list):
            port = []
        s: set[str] = set()
        for d in port:
            if not isinstance(d, dict):
                continue
            did = str(d.get("deliverable_id", "")).strip()
            if did:
                s.add(did)
        module_deliverables_cache[mid] = s
        return s

    seen_workorders_global: Dict[str, str] = {}

    def _load_ports(mid: str) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
        """Return (tenant_inputs, platform_inputs, required_tenant_inputs, tenant_output_paths)."""
        mid = str(mid).strip()
        if mid in module_ports_cache:
            ports = module_ports_cache[mid]
        else:
            myml = repo_root / "modules" / mid / "module.yml"
            if not myml.exists():
                _fail(f"Workorder references missing module folder: {mid!r}")
            cfg = _read_yaml(myml)
            ports = cfg.get("ports") or {}
            if not isinstance(ports, dict):
                _fail(f"Module {mid}: ports must be an object")
            module_ports_cache[mid] = ports

        p_in = ports.get("inputs") or {}
        p_out = ports.get("outputs") or {}
        if not isinstance(p_in, dict) or not isinstance(p_out, dict):
            _fail(f"Module {mid}: ports.inputs/ports.outputs must be objects")
        in_port = p_in.get("port") or []
        in_limited = p_in.get("limited_port") or []
        out_port = p_out.get("port") or []
        out_limited = p_out.get("limited_port") or []
        if not all(isinstance(x, list) for x in (in_port, in_limited, out_port, out_limited)):
            _fail(f"Module {mid}: ports.*.port and ports.*.limited_port must be lists")

        tenant_inputs: Set[str] = set()
        platform_inputs: Set[str] = set()
        required_tenant: Set[str] = set()
        tenant_output_paths: Set[str] = set()

        for p in in_port:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id", "")).strip()
            if not pid:
                continue
            tenant_inputs.add(pid)
            if bool(p.get("required", False)):
                required_tenant.add(pid)

        for p in in_limited:
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id", "")).strip()
            if not pid:
                continue
            platform_inputs.add(pid)

        for p in out_port:
            if not isinstance(p, dict):
                continue
            path = str(p.get("path", "")).lstrip("/").strip()
            if path:
                tenant_output_paths.add(path)

        return tenant_inputs, platform_inputs, required_tenant, tenant_output_paths

    def _is_binding(v: Any) -> bool:
        return isinstance(v, dict) and bool(v.get("from_step")) and bool(v.get("from_file"))

    for td in sorted(tenants_dir.iterdir(), key=lambda p: p.name):
        if not td.is_dir():
            continue
        tid = td.name.strip()
        validate_id("tenant_id", tid, "tenant_id")
        tyml = td / "tenant.yml"
        if not tyml.exists():
            _fail(f"Missing tenant.yml for tenant {tid}")
        cfg = _read_yaml(tyml)
        declared = str(cfg.get("tenant_id","")).strip()
        if declared and declared != tid:
            _fail(f"tenant.yml tenant_id mismatch: folder={tid!r} declared={declared!r}")

        wdir = td / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            wo = _read_yaml(wp)
            wid = str(wo.get("work_order_id", wp.stem)).strip()
            validate_id("work_order_id", wid, "work_order_id")
            if wp.stem != wid:
                _fail(f"Workorder filename mismatch: {wp.name} declared work_order_id={wid!r}")

            # Global uniqueness: work_order_id must be unique across all tenants.
            if wid in seen_workorders_global and seen_workorders_global[wid] != tid:
                _fail(f"work_order_id must be globally unique: {wid!r} used by tenants {seen_workorders_global[wid]!r} and {tid!r}")
            seen_workorders_global[wid] = tid

            if "modules" in wo and (wo.get("modules") not in (None, [], "")):
                _fail(f"Legacy workorders with 'modules' are not supported: {wp}")

            # Steps-only workorders: all chaining/wiring is expressed here.
            if "modules" in wo and wo.get("modules") not in (None, [], ""):
                _fail(f"Legacy workorder 'modules' is not supported: {wp}")

            steps = wo.get("steps")
            if not (isinstance(steps, list) and steps):
                _fail(f"Workorder must define non-empty steps list: {wp}")

            step_id_set: Set[str] = set()
            step_to_module: Dict[str, str] = {}
            step_outputs: Dict[str, Set[str]] = {}

            # First pass: ids, module ids, input keys, required inputs
            for s in steps:
                if not isinstance(s, dict):
                    _fail(f"Invalid step entry in {wp}: expected mapping")
                sid = str(s.get("step_id", "")).strip()
                try:
                    validate_id("step_id", sid, "workorder.step.step_id")
                except Exception:
                    _fail(f"Invalid step_id {sid!r} in {wp} (expected Base62 length 2)")
                if sid in step_id_set:
                    _fail(f"Duplicate step_id {sid!r} in {wp}")
                step_id_set.add(sid)

                # Human-friendly name (must not be used for logic).
                sname = s.get("step_name", None)
                if sname is None:
                    sname = s.get("name", None)
                sname = "" if sname is None else str(sname)
                if not sname.strip():
                    _fail(f"Missing required step_name for step_id={sid!r} in {wp}")
                if "\n" in sname or "\r" in sname:
                    _fail(f"Invalid step_name (newline) for step_id={sid!r} in {wp}")
                if len(sname.strip()) > 80:
                    _fail(f"Invalid step_name (too long) for step_id={sid!r} in {wp} (max 80 chars)")

                mid = str(s.get("module_id", "")).strip()
                validate_id("module_id", mid, "workorder.step.module_id")

                # Milestone 8A: deliverables must be explicitly declared per step and exist in module contract.
                deliverables = s.get("deliverables")
                if not (isinstance(deliverables, list) and deliverables):
                    _fail(f"Workorder step must define non-empty deliverables list: {wp} step_id={sid!r} module_id={mid!r}")
                cleaned = [str(d).strip() for d in deliverables if str(d).strip()]
                if not cleaned:
                    _fail(f"Workorder step deliverables list is empty after normalization: {wp} step_id={sid!r} module_id={mid!r}")
                deliverable_re = re.compile(r"^[A-Za-z0-9_]+$")
                for did in cleaned:
                    if did == "__run__":
                        _fail(f"Workorder step deliverables must not include __run__: {wp} step_id={sid!r}")
                    if not deliverable_re.match(did):
                        _fail(f"Workorder step deliverables invalid id format: {did!r} in {wp} (allowed: [A-Za-z0-9_]+)")
                allowed = _load_deliverables(mid)
                if not allowed:
                    _fail(f"Module {mid} has no deliverables contract in module.yml, but workorder references deliverables: {wp}")
                for did in cleaned:
                    if did not in allowed:
                        _fail(f"Workorder references undefined deliverable {did!r} for module {mid} in {wp}")
                step_to_module[sid] = mid

                tenant_ins, platform_ins, required_ins, tenant_out_paths = _load_ports(mid)
                step_outputs[sid] = tenant_out_paths

                inputs = s.get("inputs") or {}
                if not isinstance(inputs, dict):
                    _fail(f"Invalid step.inputs in {wp}: step_id={sid!r}")

                for k in inputs.keys():
                    if k in platform_ins:
                        _fail(f"Step {sid!r} in {wp}: input {k!r} is platform-only for module {mid}")
                    if tenant_ins and k not in tenant_ins:
                        _fail(f"Step {sid!r} in {wp}: unknown input {k!r} for module {mid}")

                for req in required_ins:
                    if req not in inputs:
                        _fail(f"Step {sid!r} in {wp}: missing required input {req!r} for module {mid}")

            # Second pass: validate bindings (from_step existence + output exposure)
            def _walk_bindings(v: Any) -> None:
                if _is_binding(v):
                    fr = str(v.get("from_step", "")).strip()
                    ff = str(v.get("from_file", "")).lstrip("/").strip()
                    if fr not in step_id_set:
                        _fail(f"Invalid binding from_step {fr!r} in {wp}: not in steps")
                    allowed = step_outputs.get(fr) or set()
                    if ff and allowed and ff not in allowed:
                        _fail(f"Invalid binding from_file {ff!r} in {wp}: not exposed by step {fr!r}")
                if isinstance(v, dict):
                    for vv in v.values():
                        _walk_bindings(vv)
                elif isinstance(v, list):
                    for vv in v:
                        _walk_bindings(vv)

            for s in steps:
                inputs = s.get("inputs") or {}
                _walk_bindings(inputs)

    _ok("Tenants + workorders: IDs + filenames OK")



