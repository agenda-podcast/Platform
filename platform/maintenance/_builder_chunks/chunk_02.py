# Generated. Do not edit by hand.
CHUNK = r'''\
    existing_hash: Dict[str, str] = {}
    for r in existing:
        mid = canon_module_id(r.get("module_id", ""))
        if not mid:
            continue
        existing_by_module.setdefault(mid, []).append(r)
        h = str(r.get("module_hash", "")).strip()
        if h:
            existing_hash[mid] = h

    out_rows: List[Dict[str, Any]] = []
    for m in modules:
        mid = m["module_id"]
        new_hash = _compute_module_hash(ctx, mid)
        if existing_hash.get(mid) == new_hash and mid in existing_by_module:
            # Preserve existing rows to keep file stable.
            out_rows.extend(existing_by_module[mid])
        else:
            out_rows.extend(_compile_module_contract_rules(ctx, mid))

    header = [
        "module_id",
        "module_hash",
        "io",
        "port_scope",
        "field_name",
        "field_id",
        "type",
        "item_type",
        "format",
        "required",
        "default_json",
        "min_value",
        "max_value",
        "min_length",
        "max_length",
        "min_items",
        "max_items",
        "regex",
        "enum_json",
        "description",
        "examples_json",
        "path",
        "content_schema_json",
        "binding_json",
        "rule_json",
        "platform_limit_json",
    ]

    # Stable sort for the full file too.
    out_rows = sorted(
        out_rows,
        key=lambda r: (
            str(r.get("module_id", "")),
            str(r.get("io", "")),
            str(r.get("port_scope", "")),
            str(r.get("field_name", "")),
        ),
    )
    _write_csv(path, out_rows, header)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]], header: List[str]) -> None:
    write_csv(path, rows, header)


def _default_category_registry() -> List[Dict[str, str]]:
    return [
        {"category_id": "01", "category_name": "Acquisition"},
        {"category_id": "12", "category_name": "Cache"},
        {"category_id": "14", "category_name": "Validation"},
        {"category_id": "15", "category_name": "Access Control"},
        {"category_id": "16", "category_name": "Billing"},
        {"category_id": "99", "category_name": "Other"},
    ]


def _ensure_category_registry(ctx: MaintenanceContext) -> Dict[str, str]:
    _ensure_dir(ctx.ids_dir)
    path = ctx.ids_dir / "category_registry.csv"
    if not path.exists():
        _write_csv(path, _default_category_registry(), ["category_id", "category_name"])
    rows = read_csv(path)
    out: Dict[str, str] = {}
    for r in rows:
        cid = str(r.get("category_id", "")).strip()
        name = str(r.get("category_name", "")).strip()
        if cid:
            out[cid] = name
    return out


def _scan_modules(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not ctx.modules_dir.exists():
        return out

    for p in sorted(ctx.modules_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        mid = p.name.strip()
        validate_id("module_id", mid, "module_id")
        module_yml = p / "module.yml"
        if not module_yml.exists():
            raise FileNotFoundError(str(module_yml))
        data = _read_yaml(module_yml)
        declared = str(data.get("module_id", "")).strip()
        if declared and declared != mid:
            raise ValueError(f"module.yml module_id mismatch: folder={mid} declared={declared}")
        supports_downloadable = bool(data.get("supports_downloadable_artifacts", True))
        out.append({
            "module_id": mid,
            "supports_downloadable_artifacts": supports_downloadable,
        })
    return out




def _write_modules_index(ctx: MaintenanceContext) -> None:
    """Write maintenance-state/modules_index.csv.

    Purpose:
      - provide deterministic, stable module options for UI dropdowns
      - avoid scanning modules/ at runtime for interactive workflows

    Design:
      - stable sort by module_id
      - include commonly used display fields (name, kind, version)
    """
    rows = []
    if not ctx.modules_dir.exists():
        _write_csv(ctx.ms_dir / "modules_index.csv", [], [
            "module_id",
            "name",
            "kind",
            "version",
            "supports_downloadable_artifacts",
            "path",
        ])
        return

    for p in sorted(ctx.modules_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        mid = p.name.strip()
        if not mid:
            continue
        validate_id("module_id", mid, "module_id")

        module_yml = p / "module.yml"
        if not module_yml.exists():
            raise FileNotFoundError(str(module_yml))
        data = _read_yaml(module_yml)
        declared = str(data.get("module_id", "") or "").strip()
        if declared and declared != mid:
            raise ValueError(f"module.yml module_id mismatch: folder={mid} declared={declared}")

        name = str(data.get("name", "") or "").strip()
        kind = str(data.get("kind", "") or "").strip()
        version = str(data.get("version", "") or "").strip()
        supports_downloadable = bool(data.get("supports_downloadable_artifacts", True))

        rows.append({
            "module_id": mid,
            "name": name,
            "kind": kind,
            "version": version,
            "supports_downloadable_artifacts": "true" if supports_downloadable else "false",
            "path": str(p.relative_to(ctx.repo_root)),
        })

    rows = sorted(rows, key=lambda r: (r.get("module_id", ""), r.get("name", "")))
    _write_csv(ctx.ms_dir / "modules_index.csv", rows, [
        "module_id",
        "name",
        "kind",
        "version",
        "supports_downloadable_artifacts",
        "path",
    ])


def _scan_tenants(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not ctx.tenants_dir.exists():
        return out
    for p in sorted(ctx.tenants_dir.iterdir(), key=lambda x: x.name):
        if not p.is_dir():
            continue
        tenant_yml = p / "tenant.yml"
        if not tenant_yml.exists():
            continue
        data = _read_yaml(tenant_yml)
        tid = canon_tenant_id(data.get("tenant_id", p.name))
        if not tid:
            continue
        validate_id("tenant_id", tid, "tenant_id")
        consumers = [canon_tenant_id(x) for x in (data.get("allow_release_consumers") or [])]
        consumers = [c for c in consumers if c]
        out.append({"tenant_id": tid, "allow_release_consumers": consumers})
    return out




def _normalize_workorder_filenames(ctx: MaintenanceContext, tenants: List[Dict[str, Any]]) -> None:
    """Rename tenant workorder files to match their declared work_order_id.

    Policy:
      - Workorder filename MUST be "{work_order_id}.yml".
      - Maintenance performs deterministic renames so tenants do not have to.
      - If a rename would overwrite an existing file, fail fast.

    This function is intentionally side-effecting (filesystem rename) and should be
    called before building workorders_index.csv.
    """
    import hashlib

    planned: list[tuple[Path, Path, Path]] = []  # (src, tmp, dst)
    dsts: set[Path] = set()

    for t in tenants:
        tid = str(t.get("tenant_id", "") or "").strip()
        if not tid:
            continue
        wdir = ctx.tenants_dir / tid / "workorders"
        if not wdir.exists():
            continue

        for wp in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            wo = _read_yaml(wp)
            wid = str(wo.get("work_order_id") or wp.stem).strip()
            if not wid:
                continue
            validate_id("work_order_id", wid, "work_order_id")

            expected = wdir / f"{wid}.yml"
            if wp.resolve() == expected.resolve():
                continue

            # Do not allow overwrite.
            if expected.exists() and expected.resolve() != wp.resolve():
                raise ValueError(
                    f"Workorder filename normalization would overwrite existing file: "
                    f"src={wp} wid={wid} dst={expected}"
                )

            # Plan a 2-phase rename to avoid swaps.
            h = hashlib.sha1(wp.name.encode("utf-8")).hexdigest()[:8]
            tmp = wdir / f".rename_tmp_{wid}_{h}.yml"
            planned.append((wp, tmp, expected))
            if expected in dsts:
                raise ValueError(f"Multiple workorders resolve to the same filename: {expected}")
            dsts.add(expected)

    if not planned:
        return

    # Phase 1: src -> tmp
    for src_p, tmp_p, _ in sorted(planned, key=lambda x: (str(x[0]), str(x[2]))):
        if tmp_p.exists():
            tmp_p.unlink()
        src_p.rename(tmp_p)

    # Phase 2: tmp -> dst
    for _, tmp_p, dst_p in sorted(planned, key=lambda x: (str(x[2]), str(x[1]))):
        if dst_p.exists() and dst_p.resolve() != tmp_p.resolve():
            raise ValueError(f"Unexpected existing destination during rename: {dst_p}")
        tmp_p.rename(dst_p)


def _write_workorders_index(ctx: MaintenanceContext, tenants: List[Dict[str, Any]]) -> None:
    """Write maintenance-state/workorders_index.csv.

    Purpose:
      - enforce global uniqueness of work_order_id across all tenants
      - provide a centralized queue/index for Orchestrator and preflight workflows
    """
    rows: List[Dict[str, str]] = []
    seen: Dict[str, str] = {}
    for t in tenants:
        tid = str(t.get("tenant_id", "") or "").strip()
        if not tid:
            continue
        wdir = ctx.tenants_dir / tid / "workorders"
        if not wdir.exists():
            continue
        for wp in sorted(wdir.glob("*.yml"), key=lambda p: p.name):
            wo = _read_yaml(wp)
            wid = str(wo.get("work_order_id") or wp.stem).strip()
            if not wid:
                continue
            validate_id("work_order_id", wid, "work_order_id")
            # Enforce global uniqueness across tenants
            if wid in seen and seen[wid] != tid:
                raise ValueError(f"work_order_id must be globally unique: {wid} used by tenants {seen[wid]} and {tid}")
            seen[wid] = tid

            enabled = bool(wo.get("enabled", True))
            meta = wo.get("metadata") or {}
            title = str(meta.get("title", "") or "").strip()
            notes = str(meta.get("notes", "") or "").strip()
            schedule = str(wo.get("schedule_cron", "") or wo.get("cron", "") or "").strip()

            rows.append({
                "tenant_id": tid,
                "work_order_id": wid,
                "enabled": "true" if enabled else "false",
                "schedule_cron": schedule,
                "title": title,
                "notes": notes,
                "path": str(wp.relative_to(ctx.repo_root)),
            })

    rows = sorted(rows, key=lambda r: (r["enabled"] != "true", r["tenant_id"], r["work_order_id"]))
    _write_csv(ctx.ms_dir / "workorders_index.csv", rows, [
        "tenant_id",
        "work_order_id",
        "enabled",
        "schedule_cron",
        "title",
        "notes",
        "path",
    ])


def _load_global_reasons(ctx: MaintenanceContext) -> List[Dict[str, Any]]:
    cfg = _read_yaml(ctx.config_dir / "global_reasons.yml")
    return list(cfg.get("reasons") or [])


def _load_module_reasons(ctx: MaintenanceContext, module_id: str) -> List[Dict[str, Any]]:
    vpath = ctx.modules_dir / module_id / "validation.yml"
    if not vpath.exists():
        return []
    cfg = _read_yaml(vpath)
    return list(cfg.get("reasons") or [])


def _normalize_reason(scope: str, module_id: str, raw: Dict[str, Any]) -> Dict[str, str]:
    rk = str(raw.get("reason_key", "")).strip()
    rs = str(raw.get("reason_slug", "")).strip()
    desc = str(raw.get("description", "")).strip()
    cat = str(raw.get("category_id", "")).strip() or ("16" if scope == "GLOBAL" else "01")

    validate_id("reason_key", rk, "reason_key")
    if not rs:
        raise ValueError("Missing reason_slug")
    if not desc:
        raise ValueError(f"Missing description for reason_slug={rs}")
    if not (len(cat) == 2 and cat.isdigit()):
        raise ValueError(f"Invalid category_id for reason_slug={rs}: {cat!r} (expected 2 digits)")

    if scope == "GLOBAL":
        mod = ""
    else:
        validate_id("module_id", module_id, "module_id")
        mod = module_id

    return {
        "scope": scope,
        "module_id": mod,
        "reason_key": rk,
        "reason_slug": rs,
        "category_id": cat,
        "description": desc,
    }


def _collect_reasons(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in _load_global_reasons(ctx):
        out.append(_normalize_reason("GLOBAL", "", r))

    for m in modules:

'''

def get_chunk() -> str:
    return CHUNK
