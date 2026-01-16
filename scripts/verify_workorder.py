#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from platform.workorders.resolver import resolve_workorder_by_id, write_single_workorder_index

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> None:
    raise SystemExit(f"[VERIFY_WORKORDER][FAIL] {msg}")


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    proc = subprocess.run(cmd, cwd=str(_REPO_ROOT), env=env)
    if proc.returncode != 0:
        _fail(f"command failed rc={proc.returncode}: {' '.join(cmd)}")


def _write_enabled_workorder_copy(*, src_rel_path: str, out_abs_path: Path, tenant_id: str) -> str:
    """Copy a workorder YAML and force enabled=true and tenant_id.

    Returns the repo-relative path to the copied workorder.
    """
    try:
        import yaml
    except Exception as e:
        _fail(f"PyYAML is required for verify_workorder: {type(e).__name__}: {e}")

    src_abs = (_REPO_ROOT / src_rel_path).resolve()
    if not src_abs.exists():
        _fail(f"workorder file not found: {src_rel_path}")

    data = yaml.safe_load(src_abs.read_text(encoding='utf-8')) or {}
    if not isinstance(data, dict):
        _fail(f"workorder YAML must be a mapping: {src_rel_path}")
    data['enabled'] = True
    data['tenant_id'] = str(tenant_id)

    out_abs_path.parent.mkdir(parents=True, exist_ok=True)
    out_abs_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf-8')
    return str(out_abs_path.resolve().relative_to(_REPO_ROOT.resolve()))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--work-order-id', required=True)
    ap.add_argument('--runtime-dir', default='runtime')
    ap.add_argument('--billing-state-dir', default='.billing-state')
    args = ap.parse_args(argv)

    work_order_id = str(args.work_order_id).strip()
    if not work_order_id:
        _fail('work-order-id is empty')

    try:
        resolved = resolve_workorder_by_id(_REPO_ROOT, work_order_id)
    except Exception as e:
        _fail(str(e))

    print(
        f"[VERIFY_WORKORDER][OK] resolved work_order_id={resolved.work_order_id} "
        f"tenant_id={resolved.tenant_id} path={resolved.path}"
    )

    # 1) Deterministic plan/preflight validation (no execution).
    _run([sys.executable, '-m', 'platform.cli', 'integrity-validate', '--path', resolved.path])

    # 2) Real run: execute orchestrator normally, but constrain the queue to this workorder.
    runtime_dir = Path(str(args.runtime_dir or 'runtime')).resolve()
    billing_state_dir = Path(str(args.billing_state_dir or '.billing-state')).resolve()

    verify_dir = runtime_dir / 'verify_workorders' / resolved.work_order_id
    verify_dir.mkdir(parents=True, exist_ok=True)
    # The canonical queue filters by the workorder file's own enabled flag.
    # For verification, create a local copy with enabled=true.
    workorder_copy_abs = verify_dir / 'workorder.enabled.yml'
    workorder_copy_rel = _write_enabled_workorder_copy(src_rel_path=resolved.path, out_abs_path=workorder_copy_abs, tenant_id=resolved.tenant_id)

    override_index = verify_dir / 'workorders_index.single.csv'
    write_single_workorder_index(
        _REPO_ROOT,
        type(resolved)(tenant_id=resolved.tenant_id, work_order_id=resolved.work_order_id, path=workorder_copy_rel),
        override_index,
    )

    env = dict(os.environ)
    env['PLATFORM_WORKORDERS_INDEX_PATH'] = str(override_index)

    # scripts/run_orchestrator.sh also hydrates and publishes billing-state, and runs publishing/delivery "as is".
    _run(
        [
            'bash',
            str(_REPO_ROOT / 'scripts' / 'run_orchestrator.sh'),
            '--runtime-dir',
            str(runtime_dir),
            '--billing-state-dir',
            str(billing_state_dir),
        ],
        env=env,
    )

    print(f"[VERIFY_WORKORDER][OK] real-run completed: work_order_id={resolved.work_order_id}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
