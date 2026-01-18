from __future__ import annotations

try:
    from repo_bootstrap import ensure_repo_root_on_sys_path
except ModuleNotFoundError:  # pragma: no cover
    from scripts.repo_bootstrap import ensure_repo_root_on_sys_path

ensure_repo_root_on_sys_path()

"""CI verification entrypoint.

Implementation split into small modules to keep files <=500 lines.
"""

import argparse

from scripts.ci_verify_lib.core import _fail, _ok, _warn
from pathlib import Path
from typing import Optional, Sequence

from scripts.ci_verify_lib.core import _validate_repo_billing_config
from scripts.ci_verify_lib.modules import _validate_modules
from scripts.ci_verify_lib.tenants import _validate_tenants_and_workorders
from scripts.ci_verify_lib.state import _validate_maintenance_state, _validate_billing_state

def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["pre","post"], required=True)
    ap.add_argument("--billing-state-dir", default=".billing-state")
    ap.add_argument("--runtime-dir", default="runtime")
    args = ap.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    billing_state_dir = Path(args.billing_state_dir).resolve()

    if args.phase == "pre":
        _validate_repo_billing_config(repo_root)
        _validate_modules(repo_root)
        _validate_tenants_and_workorders(repo_root)
        _validate_maintenance_state(repo_root)
    else:
        _validate_billing_state(billing_state_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
