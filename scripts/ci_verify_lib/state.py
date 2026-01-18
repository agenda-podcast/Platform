from __future__ import annotations

from .core import *

def _validate_maintenance_state(repo_root: Path) -> None:
    ms = repo_root / "maintenance-state"
    required = [
        "reason_catalog.csv",
        "reason_policy.csv",
        "tenant_relationships.csv",
        "workorders_index.csv",
        "modules_index.csv",
        "module_requirements_index.csv",
        "module_artifacts_policy.csv",
        "module_contract_rules.csv",
        "platform_policy.csv",
        "maintenance_manifest.csv",
        "ids/category_registry.csv",
        "ids/reason_registry.csv",
    ]
    for rel in required:
        p = ms / rel
        if not p.exists():
            _fail(f"Missing maintenance-state file: {rel}")

    # spot-check IDs in reason catalog/registry
    cat = read_csv(ms / "reason_catalog.csv")
    for r in cat:
        rc = str(r.get("reason_code","")).strip()
        if rc:
            validate_id("reason_code", rc, "reason_code")
        rk = str(r.get("reason_key","")).strip()
        if rk:
            validate_id("reason_key", rk, "reason_key")
        scope = str(r.get("scope","")).strip().upper()
        if scope not in ("GLOBAL","MODULE"):
            _fail(f"Invalid reason scope: {scope!r}")
        mid = str(r.get("module_id","")).strip()
        if scope == "MODULE":
            validate_id("module_id", mid, "module_id")
        elif mid:
            _fail("Global reason has non-empty module_id")
    # Header spot-check: modules index (used for dropdowns)
    _assert_exact_header(ms / "modules_index.csv", ["module_id","name","kind","version","supports_downloadable_artifacts","path"])


    _ok("Maintenance-state: required files + ID format OK")


def _validate_billing_state(billing_state_dir: Path) -> None:
    required_files = [
        "tenants_credits.csv",
        "transactions.csv",
        "transaction_items.csv",
        "promotion_redemptions.csv",
        "cache_index.csv",
                        "github_releases_map.csv",
        "github_assets_map.csv",
        "state_manifest.json",
    ]
    for fn in required_files:
        p = billing_state_dir / fn
        if not p.exists():
            _fail(f"Billing-state missing required file: {p}")

    # headers
    _assert_exact_header(billing_state_dir / "tenants_credits.csv", ["tenant_id","credits_available","updated_at","status"])
    _assert_exact_header(billing_state_dir / "transactions.csv", ["transaction_id","tenant_id","work_order_id","type","amount_credits","created_at","reason_code","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "transaction_items.csv", ["transaction_item_id","transaction_id","tenant_id","module_id","work_order_id","step_id","deliverable_id","feature","type","amount_credits","created_at","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "promotion_redemptions.csv", ["redemption_id","tenant_id","promo_code","credits_granted","created_at","note","metadata_json"])
    _assert_exact_header(billing_state_dir / "cache_index.csv", ["place","type","ref","created_at","expires_at"])
    _assert_exact_header(billing_state_dir / "github_releases_map.csv", ["release_id","github_release_id","tag","tenant_id","work_order_id","created_at"])
    _assert_exact_header(billing_state_dir / "github_assets_map.csv", ["asset_id","github_asset_id","release_id","asset_name","created_at"])

    # ID format sanity on non-empty rows
    for r in read_csv(billing_state_dir / "tenants_credits.csv"):
        tid = str(r.get("tenant_id","")).strip()
        if tid:
            validate_id("tenant_id", tid, "tenant_id")

    for r in read_csv(billing_state_dir / "transactions.csv"):
        if r.get("transaction_id"):
            validate_id("transaction_id", str(r["transaction_id"]).strip(), "transaction_id")
        if r.get("tenant_id"):
            validate_id("tenant_id", str(r["tenant_id"]).strip(), "tenant_id")
        wid = str(r.get("work_order_id","")).strip()
        if wid:
            validate_id("work_order_id", wid, "work_order_id")

    _ok("Billing-state: required assets + headers + basic ID format OK")



