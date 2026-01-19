from __future__ import annotations

import os
import subprocess
from pathlib import Path
import sys


def _env(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def main() -> int:
    """Maintenance entrypoint.

    Deterministic policy:
    - Billing GitHub Release assets are the Source of Truth.
    - Repository templates are scaffolds only, used only to seed *missing* assets.
    - Maintenance must never overwrite existing billing Release assets.
    """

    try:
        from platform.billing import publish_default_billing_release
    except Exception as e:
        print("Error: Billing release bootstrap module not found: platform.billing.publish_default_billing_release")
        print(str(e))
        return 2

    billing_tag = _env("BILLING_TAG", "billing-state-v1")
    template_dir = Path(_env("BILLING_TEMPLATE_DIR", "releases/billing-state-v1"))
    billing_state_dir = Path(_env("BILLING_STATE_DIR", ".billing-state"))

    print("[maintenance] Start")
    print(f"[maintenance] BILLING_TAG={billing_tag}")
    print(f"[maintenance] BILLING_TEMPLATE_DIR={template_dir}")
    print(f"[maintenance] BILLING_STATE_DIR={billing_state_dir}")

    # 1) Ensure Release exists and seed missing assets only.
    rc = publish_default_billing_release.main()
    if rc != 0:
        print(f"[maintenance] Billing release ensure failed with rc={rc}")
        return int(rc)

    # 2) Hydrate local billing-state from Release assets (SoT).
    # In CI, scripts/billing_state_hydrate.py enforces require-release by default.
    _run(
        [
            sys.executable,
            "scripts/billing_state_hydrate.py",
            "--billing-state-dir",
            str(billing_state_dir),
            "--scaffold-dir",
            str(template_dir),
            "--release-tag",
            billing_tag,
        ]
    )
    print("[maintenance] Local billing-state hydrate from Release: OK")

    print("[maintenance] Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
