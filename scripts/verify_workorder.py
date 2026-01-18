#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from platform.workorders.resolver import resolve_workorder_by_id, write_single_workorder_index


def _run(cmd: List[str], env: dict, cwd: Path) -> Tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out = proc.stdout or ""
    # stream output to console for GitHub Actions logs
    sys.stdout.write(out)
    sys.stdout.flush()
    return int(proc.returncode), out


def _is_failure_from_output(out: str) -> Optional[str]:
    # Deterministic fail signals we must treat as workflow failure.
    needles = [
        ("[preflight][FAILED]", "preflight failed (missing secrets or invalid inputs)"),
        ("ConsistencyValidationError:", "integrity validation failed"),
        ("[BILLING_PUBLISH][SKIP] missing repo/token", "billing publish skipped (missing repo/token)"),
        ("deliver_github_release", "delivery module referenced"),  # used as context only
    ]

    if "[preflight][FAILED]" in out:
        return "preflight failed (missing secrets or invalid inputs)"
    if "ConsistencyValidationError:" in out:
        return "integrity validation failed"
    if "[BILLING_PUBLISH][SKIP] missing repo/token" in out:
        return "billing publish skipped (missing repo/token)"
    # If the orchestrator reports FAILED in its own structured logs
    if "[orchestrator][FAILED]" in out:
        return "orchestrator reported FAILED"
    return None


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-order-id", required=True)
    ap.add_argument("--runtime-dir", default="runtime")
    ap.add_argument("--billing-state-dir", default=".billing-state")
    args = ap.parse_args(argv)

    env = os.environ.copy()

    # Resolve workorder path from maintenance-state/workorders_index.csv
    resolved = resolve_workorder_by_id(_REPO_ROOT, args.work_order_id)
    workorder_path = (_REPO_ROOT / resolved.path).resolve()

    # 1) Integrity validate workorder (must be deterministic)
    rc, out = _run(
        [sys.executable, "-m", "platform.cli", "integrity-validate", "--path", str(workorder_path)],
        env=env,
        cwd=_REPO_ROOT,
    )
    if rc != 0:
        print(f"[VERIFY_WORKORDER][FAIL] integrity-validate failed rc={rc}")
        return rc

    # 2) Create a single-row workorders_index.csv under runtime so orchestrator queues exactly one workorder.
    runtime_dir = (_REPO_ROOT / args.runtime_dir / "verify_workorders" / resolved.work_order_id).resolve()
    runtime_dir.mkdir(parents=True, exist_ok=True)

    single_index = runtime_dir / "workorders_index.single.csv"
    write_single_workorder_index(_REPO_ROOT, resolved, single_index)

    # 3) Run orchestrator through wrapper (ensures billing bootstrap + billing publish)
    rc, out = _run(
        [
            str(_REPO_ROOT / "scripts" / "run_orchestrator.sh"),
            "--runtime-dir",
            str(runtime_dir.relative_to(_REPO_ROOT)),
            "--billing-state-dir",
            str((_REPO_ROOT / args.billing_state_dir).resolve().relative_to(_REPO_ROOT)),
            "--queue-source",
            str(single_index.relative_to(_REPO_ROOT)),
        ],
        env=env,
        cwd=_REPO_ROOT,
    )

    # Treat any non-zero rc as fail
    if rc != 0:
        print(f"[VERIFY_WORKORDER][FAIL] orchestrator wrapper failed rc={rc}")
        reason = _is_failure_from_output(out)
        if reason:
            print(f"[VERIFY_WORKORDER][FAIL] reason={reason}")
        return rc

    # Even with rc=0, enforce that preflight did not fail and billing publish did not skip.
    reason = _is_failure_from_output(out)
    if reason:
        print(f"[VERIFY_WORKORDER][FAIL] reason={reason}")
        return 2

    # Basic success marker: delivery receipt should be present for delivery steps
    if "deliver_github_release" in out and "delivery" in out and "[DELIVER_" not in out:
        # Not a hard proof, but catches common silent no-op patterns.
        print("[VERIFY_WORKORDER][WARN] delivery module referenced but no delivery log markers found")

    print(f"[VERIFY_WORKORDER][OK] real-run completed: work_order_id={resolved.work_order_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
