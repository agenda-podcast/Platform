from __future__ import annotations

import sys
from pathlib import Path

# Ensure local 'platform' package shadows stdlib 'platform'
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.modules.pop('platform', None)


import shutil

from platform.infra.config import load_runtime_profile
from platform.infra.factory import build_infra
from platform.orchestration.orchestrator import run_orchestrator
from platform.utils.csvio import read_csv


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    for fp in src.rglob("*"):
        if fp.is_dir():
            continue
        rel = fp.relative_to(src)
        outp = dst / rel
        outp.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fp, outp)


def test_orchestrator_rerun_no_duplicate_ledger_items(tmp_path: Path) -> None:
    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    # Minimal repo layout required by orchestrator
    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")

    # Dummy module
    mod_root = repo_root / "modules" / "dum"
    (mod_root / "src").mkdir(parents=True, exist_ok=True)

    (mod_root / "module.yml").write_text(
        """module_id: dum
name: Dummy
version: 1
ports:
  inputs: {}
  outputs:
    out1:
      path: tenant_outputs/out.txt
      exposure: tenant

deliverables:
  tenant_outputs:
    outputs: [out1]
    limited_inputs: []
""",
        encoding="utf-8",
    )

    (mod_root / "src" / "run.py").write_text(
        """from __future__ import annotations

from typing import Any, Dict


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    outp = outputs_dir / "tenant_outputs" / "out.txt"
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text("ok", encoding="utf-8")
    return {"status": "COMPLETED", "files": [str(outp)]}
""",
        encoding="utf-8",
    )

    # Ensure pricing exists for dummy module
    prices_path = repo_root / "platform" / "billing" / "module_prices.csv"
    existing = prices_path.read_text(encoding="utf-8")
    if "dum,__run__" not in existing:
        prices_path.write_text(existing.rstrip("\n") + "\ndum,__run__,1,2020-01-01,,true,dummy run price\n", encoding="utf-8")

    # Artifacts policy and display name (optional but reduces warnings)
    policy_path = repo_root / "maintenance-state" / "module_artifacts_policy.csv"
    if "dum," not in policy_path.read_text(encoding="utf-8"):
        policy_path.write_text(policy_path.read_text(encoding="utf-8").rstrip("\n") + "\ndum,true\n", encoding="utf-8")

    names_path = repo_root / "maintenance-state" / "module_display_names.csv"
    if names_path.exists():
        if "dum," not in names_path.read_text(encoding="utf-8"):
            names_path.write_text(names_path.read_text(encoding="utf-8").rstrip("\n") + "\ndum,Dummy\n", encoding="utf-8")

    # Tenant + workorder
    tenant_id = "tst123"
    work_order_id = "WOOO0000"
    wo_dir = repo_root / "tenants" / tenant_id / "workorders"
    wo_dir.mkdir(parents=True, exist_ok=True)

    (wo_dir / f"{work_order_id}.yml").write_text(
        """enabled: true
mode: PARTIAL_ALLOWED
steps:
  - step_id: s1
    module_id: dum
    requested_deliverables: []
""",
        encoding="utf-8",
    )

    # Queue: a simple CSV pointing to the workorder file
    queue_path = repo_root / "workorders_queue.csv"
    queue_path.write_text(
        "tenant_id,work_order_id,path\n" + f"{tenant_id},{work_order_id},tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"

    # Seed billing-state with required CSVs
    _copy_tree(repo_src / "billing-state-seed", billing_state_dir)

    profile = load_runtime_profile(repo_root)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    # First run
    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items1 = read_csv(billing_state_dir / "transaction_items.csv")

    # Second run (same inputs)
    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items2 = read_csv(billing_state_dir / "transaction_items.csv")

    # Assert: no duplicate idempotency_key values
    keys = []
    for r in items2:
        meta = r.get("metadata_json") or "{}"
        try:
            import json

            m = json.loads(meta) if meta.strip() else {}
        except Exception:
            m = {}
        k = str(m.get("idempotency_key") or "").strip()
        if k:
            keys.append(k)

    assert len(keys) == len(set(keys))
    # And: second run did not increase ledger items count
    assert len(items2) == len(items1)
