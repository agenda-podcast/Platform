# Generated. Do not edit by hand.
CHUNK = r'''\
        mid = m["module_id"]
        for r in _load_module_reasons(ctx, mid):
            out.append(_normalize_reason("MODULE", mid, r))

    # Uniqueness guarantees:
    seen_keys: set[str] = set()
    seen_slugs: set[Tuple[str, str, str]] = set()
    for r in out:
        rk = r["reason_key"]
        if rk in seen_keys:
            raise ValueError(f"Duplicate reason_key in config/validation: {rk}")
        seen_keys.add(rk)

        key = (r["scope"], r["module_id"], r["reason_slug"])
        if key in seen_slugs:
            raise ValueError(f"Duplicate reason_slug within scope/module: {key}")
        seen_slugs.add(key)

    return out


def _ensure_reason_registry(ctx: MaintenanceContext, reasons: List[Dict[str, str]]) -> List[Dict[str, str]]:
    _ensure_dir(ctx.ids_dir)
    path = ctx.ids_dir / "reason_registry.csv"
    existing = read_csv(path) if path.exists() else []
    by_scope_mod_slug: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    used_codes: set[str] = set()

    for r in existing:
        code = str(r.get("reason_code", "")).strip()
        scope = str(r.get("scope", "")).strip()
        mod = str(r.get("module_id", "")).strip()
        slug = str(r.get("reason_slug", "")).strip()
        if not (code and scope and slug):
            continue
        try:
            validate_id("reason_code", code, "reason_code")
        except Exception:
            continue
        if scope == "GLOBAL":
            mod = ""
        key = (scope, mod, slug)
        if key in by_scope_mod_slug:
            continue
        used_codes.add(code)
        by_scope_mod_slug[key] = dict(r)

    out: List[Dict[str, str]] = []
    for r in reasons:
        key = (r["scope"], r["module_id"], r["reason_slug"])
        row = by_scope_mod_slug.get(key)
        if row:
            row.update({
                "scope": r["scope"],
                "module_id": r["module_id"],
                "reason_key": r["reason_key"],
                "reason_slug": r["reason_slug"],
                "category_id": r["category_id"],
                "description": r["description"],
            })
        else:
            code = generate_unique_id("reason_code", used_codes)
            row = {
                "reason_code": code,
                "scope": r["scope"],
                "module_id": r["module_id"],
                "reason_key": r["reason_key"],
                "reason_slug": r["reason_slug"],
                "category_id": r["category_id"],
                "description": r["description"],
            }
        out.append(row)

    out = sorted(out, key=lambda x: x["reason_code"])
    _write_csv(path, out, ["reason_code","scope","module_id","reason_key","reason_slug","category_id","description"])
    return out


def _ensure_reason_policy(ctx: MaintenanceContext, reason_registry: List[Dict[str, str]]) -> List[Dict[str, str]]:
    path = ctx.ms_dir / "reason_policy.csv"
    existing = read_csv(path) if path.exists() else []
    by_code: Dict[str, Dict[str, str]] = {str(r.get("reason_code", "")).strip(): r for r in existing if r.get("reason_code")}
    out: List[Dict[str, str]] = []

    for r in reason_registry:
        code = str(r["reason_code"]).strip()
        scope = str(r["scope"]).strip()
        default_ref = "true" if scope == "MODULE" else "false"
        row = by_code.get(code, {})
        refundable = str(row.get("refundable", default_ref)).lower()
        if refundable not in ("true", "false"):
            refundable = default_ref
        out.append({
            "reason_code": code,
            "refundable": refundable,
            "notes": str(row.get("notes", "") or ""),
        })

    out = sorted(out, key=lambda x: x["reason_code"])
    _write_csv(path, out, ["reason_code","refundable","notes"])
    return out


def _write_reason_catalog(ctx: MaintenanceContext, reason_registry: List[Dict[str, str]], categories: Dict[str, str]) -> None:
    path = ctx.ms_dir / "reason_catalog.csv"
    rows: List[Dict[str, str]] = []
    for r in reason_registry:
        cat = str(r.get("category_id", "")).strip()
        rows.append({
            "reason_code": str(r.get("reason_code", "")).strip(),
            "scope": str(r.get("scope", "")).strip(),
            "module_id": str(r.get("module_id", "")).strip(),
            "reason_key": str(r.get("reason_key", "")).strip(),
            "reason_slug": str(r.get("reason_slug", "")).strip(),
            "category_id": cat,
            "category_name": categories.get(cat, ""),
            "description": str(r.get("description", "")).strip(),
        })
    rows = sorted(rows, key=lambda x: x["reason_code"])
    _write_csv(path, rows, ["reason_code","scope","module_id","reason_key","reason_slug","category_id","category_name","description"])


def _write_tenant_relationships(ctx: MaintenanceContext, tenants: List[Dict[str, Any]]) -> None:
    rows: List[Dict[str, str]] = []
    for t in tenants:
        src = t["tenant_id"]
        rows.append({"source_tenant_id": src, "target_tenant_id": src})
        for dst in t.get("allow_release_consumers") or []:
            if dst:
                rows.append({"source_tenant_id": src, "target_tenant_id": dst})
    seen = set()
    deduped=[]
    for r in rows:
        k=(r["source_tenant_id"], r["target_tenant_id"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    deduped = sorted(deduped, key=lambda x: (x["source_tenant_id"], x["target_tenant_id"]))
    _write_csv(ctx.ms_dir / "tenant_relationships.csv", deduped, ["source_tenant_id","target_tenant_id"])




def _write_module_requirements_index(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Write module_requirements_index.csv.

    Source of truth: modules/<module_id>/module.yml (requirements block).
    platform/modules/requirements.csv is deprecated and must be header-only.
    """
    legacy = read_csv(ctx.repo_root / "platform" / "modules" / "requirements.csv")
    if legacy:
        raise ValueError(
            "platform/modules/requirements.csv is deprecated and must be header-only. "
            "Move requirements to modules/<module_id>/module.yml under 'requirements:'."
        )

    module_ids = [m["module_id"] for m in modules]
    out: List[Dict[str, str]] = []

    for mid in module_ids:
        mdir = ctx.modules_dir / mid
        myml = _read_yaml(mdir / "module.yml")
        req = myml.get("requirements") or {}
        if not isinstance(req, dict):
            continue

        def _emit(req_type: str, item: Any) -> None:
            if isinstance(item, str):
                name = item.strip()
                note = ""
                val = ""
            elif isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                note = str(item.get("note") or "").strip()
                val = "" if item.get("default") is None else str(item.get("default"))
            else:
                return
            if not name:
                return
            out.append(
                {
                    "module_id": mid,
                    "requirement_type": req_type,
                    "requirement_key": name,
                    "requirement_value": val,
                    "note": note,
                }
            )

        for it in (req.get("secrets") or []):
            _emit("secret", it)
        for it in (req.get("vars") or []):
            _emit("var", it)

    _write_csv(
        ctx.ms_dir / "module_requirements_index.csv",
        out,
        ["module_id", "requirement_type", "requirement_key", "requirement_value", "note"],
    )
    return out


def _write_secretstore_template(ctx: MaintenanceContext, modules: List[Dict[str, Any]], req_rows: List[Dict[str, str]]) -> None:
    """Regenerate platform/secretstore/secretstore.template.json from module requirements.

    Template keys are module-scoped. Secret names are allowed to be non-unique across modules.
    Values are placeholders only (safe to commit).
    """
    mods: Dict[str, Dict[str, Any]] = {}
    for m in modules:
        mid = m["module_id"]
        mods[mid] = {"secrets": {}, "vars": {}}

    for r in req_rows:
        mid = (r.get("module_id") or "").strip()
        rtype = (r.get("requirement_type") or "").strip()
        key = (r.get("requirement_key") or "").strip()
        val = r.get("requirement_value")
        if not mid or not key or mid not in mods:
            continue
        if rtype == "secret":
            mods[mid]["secrets"][key] = "REPLACE_ME"
        elif rtype == "var":
            mods[mid]["vars"][key] = val if isinstance(val, str) and val != "" else "REPLACE_ME"

    integrations: Dict[str, Dict[str, Any]] = {}

    # Integration requirements derived from runtime profiles.
    # These are platform-level secrets/vars that are not tied to a module.
    # They live under the top-level "integrations" key in secretstore JSON.
    runtime_profiles = sorted(ctx.config_dir.glob("runtime_profile*.yml"))
    required_integ: Dict[str, Dict[str, List[str]]] = {}

    has_dropbox_delivery = any(str(m.get("module_id", "")).strip() == "deliver_dropbox" for m in modules)

    def _add_integration_req(integration_id: str, *, secrets: List[str], vars: List[str]) -> None:
        blk = required_integ.setdefault(integration_id, {"secrets": [], "vars": []})
        for s in secrets:
            if s not in blk["secrets"]:
                blk["secrets"].append(s)
        for v in vars:
            if v not in blk["vars"]:
                blk["vars"].append(v)

    def _extract_kinds(adapter_obj: Any) -> List[str]:
        if not isinstance(adapter_obj, dict):
            return []
        kind = str(adapter_obj.get("kind", "") or "").strip()
        if kind and kind != "multi":
            return [kind]
        if kind == "multi":
            inner = adapter_obj.get("stores") or adapter_obj.get("publishers") or []
            out: List[str] = []
            if isinstance(inner, list):
                for it in inner:
                    if isinstance(it, dict):
                        k = str(it.get("kind", "") or "").strip()
                        if k:
                            out.append(k)
            return out
        return []

    for rp in runtime_profiles:
        try:
            y = _read_yaml(rp)
        except Exception:
            continue
        adapters = y.get("adapters") or {}
        if not isinstance(adapters, dict):
            continue
        # Artifact store requirements
        kinds = _extract_kinds(adapters.get("artifact_store") or {})
        if "s3" in kinds:
            _add_integration_req(
                "artifact_store_s3",
                secrets=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
                vars=["AWS_DEFAULT_REGION", "PLATFORM_ARTIFACTS_S3_BUCKET", "PLATFORM_ARTIFACTS_S3_PREFIX"],
            )

        # OAuth / tenant credentials requirements
        tcs = adapters.get("tenant_credentials_store")
        if isinstance(tcs, dict) and str(tcs.get("kind", "") or "").strip():
            _add_integration_req(
                "oauth_global",
                secrets=["OAUTH_STATE_SIGNING_KEY", "TOKEN_ENCRYPTION_KEY"],
                vars=[],
            )
            if has_dropbox_delivery:
                _add_integration_req(
                    "oauth_dropbox",
                    secrets=["DROPBOX_APP_KEY", "DROPBOX_APP_SECRET"],
                    vars=["DROPBOX_SCOPES"],
                )

    for integration_id, blk in sorted(required_integ.items(), key=lambda x: x[0]):
        integrations[integration_id] = {"secrets": {}, "vars": {}}
        for k in sorted(blk.get("secrets") or []):
            integrations[integration_id]["secrets"][k] = "REPLACE_ME"
        for k in sorted(blk.get("vars") or []):
            integrations[integration_id]["vars"][k] = "REPLACE_ME"

    payload = {"version": 1, "generated_at": "MAINTENANCE", "modules": mods, "integrations": integrations}
    out_path = ctx.repo_root / "platform" / "secretstore" / "secretstore.template.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def _write_module_artifacts_policy(ctx: MaintenanceContext, modules: List[Dict[str, Any]]) -> None:
    cfg = _read_yaml(ctx.config_dir / "platform_policy.yml")
    default_enabled = bool(cfg.get("platform_artifacts_enabled_default", True))
    rows=[]
    for m in modules:
        enabled = default_enabled and bool(m.get("supports_downloadable_artifacts", True))
        rows.append({
            "module_id": m["module_id"],
            "platform_artifacts_enabled": "true" if enabled else "false",
            "notes": "",
        })
    rows = sorted(rows, key=lambda x: x["module_id"])
    _write_csv(ctx.ms_dir / "module_artifacts_policy.csv", rows, ["module_id","platform_artifacts_enabled","notes"])


def _write_platform_policy(ctx: MaintenanceContext) -> None:
    cfg = _read_yaml(ctx.config_dir / "platform_policy.yml")
    rows=[]
    for k,v in cfg.items():
        rows.append({"policy_key": str(k), "policy_value": json.dumps(v) if isinstance(v,(dict,list,bool)) else str(v)})
    _write_csv(ctx.ms_dir / "platform_policy.csv", rows, ["policy_key","policy_value"])


def _write_manifest(ctx: MaintenanceContext) -> None:
    # IMPORTANT: This manifest must be *stable* across runs when underlying
    # file contents have not changed. Otherwise CI will always detect drift.
    #
    # Rule: preserve updated_at if sha256 is unchanged from the prior manifest.
    existing_path = ctx.ms_dir / "maintenance_manifest.csv"
    existing_rows = read_csv(existing_path) if existing_path.exists() else []
    existing_by_file: Dict[str, Dict[str, str]] = {}
    for r in existing_rows:
        fn = str(r.get("file", "")).strip()
        sha = str(r.get("sha256", "")).strip()
        ts = str(r.get("updated_at", "")).strip()
        if fn and sha and ts:
            existing_by_file[fn] = {"sha256": sha, "updated_at": ts}

    files = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "workorders_index.csv",
        "modules_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "module_contract_rules.csv",
        "platform_policy.csv",
    ]
    rows=[]
    for fn in files:
        p = ctx.ms_dir / fn
        sha = _sha256_file(p)
        prior = existing_by_file.get(fn)
        if prior and prior.get("sha256") == sha:
            ts = prior.get("updated_at", "")
        else:
            ts = utcnow_iso()
        rows.append({"file": fn, "sha256": sha, "updated_at": ts})
    _write_csv(ctx.ms_dir / "maintenance_manifest.csv", rows, ["file","sha256","updated_at"])


def run_maintenance(repo_root: Path) -> None:
    ctx = MaintenanceContext(repo_root=repo_root)
    _ensure_dir(ctx.ms_dir)
    categories = _ensure_category_registry(ctx)

    modules = _scan_modules(ctx)
    tenants = _scan_tenants(ctx)

    reasons = _collect_reasons(ctx, modules)
    reason_registry = _ensure_reason_registry(ctx, reasons)


'''

def get_chunk() -> str:
    return CHUNK
