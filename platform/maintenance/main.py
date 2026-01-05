from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _env(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def _repo_root_from_here() -> Path:
    # platform/maintenance/main.py -> repo root
    return Path(__file__).resolve().parents[2]


def _abs_from_repo(repo_root: Path, p: str) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (repo_root / pp)


def _hydrate_local_billing_state(repo_root: Path, *, billing_state_dir: Path, scaffold_dir: Path, billing_tag: str) -> None:
    hydrate_script = repo_root / "scripts" / "billing_state_hydrate.py"
    if not hydrate_script.exists():
        raise FileNotFoundError(f"Missing hydrate script: {hydrate_script}")

    cmd = [
        sys.executable,
        str(hydrate_script),
        "--billing-state-dir",
        str(billing_state_dir),
        "--scaffold-dir",
        str(scaffold_dir),
        "--release-tag",
        billing_tag,
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Billing-state hydration failed (rc={p.returncode}). Output:\n{p.stdout}")


def main() -> int:
    # 1) Ensure billing-state GitHub release exists and has required assets.
    try:
        from platform.billing import publish_default_billing_release
    except Exception as e:
        print("Error: Billing release bootstrap module not found: platform.billing.publish_default_billing_release")
        print(str(e))
        return 2

    repo_root = _repo_root_from_here()

    billing_tag = _env("BILLING_TAG", "billing-state-v1")
    # Scaffold is only a per-file fallback for fresh-start; Release is Source of Truth.
    scaffold_dir = _abs_from_repo(repo_root, _env("BILLING_TEMPLATE_DIR", "releases/billing-state-v1"))
    billing_state_dir = _abs_from_repo(repo_root, _env("BILLING_STATE_DIR", ".billing-state"))

    print("[maintenance] Start")
    print(f"[maintenance] BILLING_TAG={billing_tag}")
    print(f"[maintenance] BILLING_TEMPLATE_DIR={scaffold_dir}")
    print(f"[maintenance] BILLING_STATE_DIR={billing_state_dir}")

    # Publish/ensure Release assets (idempotent)
    rc = publish_default_billing_release.main()
    if rc != 0:
        print(f"[maintenance] Billing release ensure failed with rc={rc}")
        return int(rc)

    # 2) Hydrate local billing-state:
    #    - Prefer Release assets (clobber local)
    #    - Fall back per-file to scaffold for missing assets (fresh-start)
    try:
        _hydrate_local_billing_state(
            repo_root,
            billing_state_dir=billing_state_dir,
            scaffold_dir=scaffold_dir,
            billing_tag=billing_tag,
        )
    except Exception as e:
        print(str(e))
        return 2

    print("[maintenance] Local billing-state hydration: OK")
    print("[maintenance] Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
