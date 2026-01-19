from __future__ import annotations

import os
import subprocess
from pathlib import Path
import sys

REQUIRED_FILES = [
    "tenants_credits.csv",
    "transactions.csv",
    "transaction_items.csv",
    "promotion_redemptions.csv",
    "cache_index.csv",
            "github_releases_map.csv",
    "github_assets_map.csv",
    "state_manifest.json",
]


def _env(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def _hydrate_local_billing_state(billing_tag: str, billing_state_dir: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    script = repo_root / "scripts" / "billing_state_hydrate.py"
    if not script.exists():
        raise FileNotFoundError(f"Missing required script: {script}")

    cmd = [
        sys.executable,
        str(script),
        "--billing-state-dir",
        str(billing_state_dir),
        "--release-tag",
        billing_tag,
        "--require-release",
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(p.stdout)
    if p.returncode != 0:
        raise RuntimeError(f"billing_state_hydrate failed rc={p.returncode}")


def main() -> int:
    # 1) Ensure billing-state GitHub release exists and has required assets
    #    This import is expected to exist from earlier patches; if it doesn't,
    #    we fail loudly so CI doesn't "green" with no work performed.
    try:
        from platform.billing import publish_default_billing_release
    except Exception as e:
        print("Error: Billing release bootstrap module not found: platform.billing.publish_default_billing_release")
        print(str(e))
        return 2

    # Env
    billing_tag = _env("BILLING_TAG", "billing-state-v1")
    template_dir = Path(_env("BILLING_TEMPLATE_DIR", "releases/billing-state-v1"))
    billing_state_dir = Path(_env("BILLING_STATE_DIR", ".billing-state"))

    print(f"[maintenance] Start")
    print(f"[maintenance] BILLING_TAG={billing_tag}")
    print(f"[maintenance] BILLING_TEMPLATE_DIR={template_dir}")
    print(f"[maintenance] BILLING_STATE_DIR={billing_state_dir}")

    # Publish/ensure Release assets (idempotent)
    # publish_default_billing_release reads GITHUB_TOKEN + GITHUB_REPOSITORY from env
    rc = publish_default_billing_release.main()
    if rc != 0:
        print(f"[maintenance] Billing release ensure failed with rc={rc}")
        return int(rc)

    # 2) Hydrate local billing-state directory from Release (source of truth)
    #    This prevents Maintenance from ever overwriting real ledger data with checked-in templates.
    _hydrate_local_billing_state(billing_tag, billing_state_dir)
    print("[maintenance] Local billing-state hydrate: OK")

    print("[maintenance] Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
