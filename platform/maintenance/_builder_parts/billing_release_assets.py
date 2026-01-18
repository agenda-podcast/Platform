# Generated. Do not edit by hand.
CHUNK = r'''\
    _write_reason_catalog(ctx, reason_registry, categories)
    _ensure_reason_policy(ctx, reason_registry)

    _write_tenant_relationships(ctx, tenants)
    _write_modules_index(ctx)
    _normalize_workorder_filenames(ctx, tenants)
    _write_workorders_index(ctx, tenants)
    req_rows = _write_module_requirements_index(ctx, modules)
    _write_secretstore_template(ctx, modules, req_rows)
    _write_module_artifacts_policy(ctx, modules)
    _write_module_contract_rules(ctx, modules)
    _write_platform_policy(ctx)
    _write_manifest(ctx)

'''

def get_part() -> str:
    return CHUNK
