from __future__ import annotations

import json
import shutil
import textwrap
from pathlib import Path

from _testutil import ensure_repo_on_path


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


def test_orchestrator_packaging_and_delivery_create_charges_and_are_idempotent(tmp_path: Path) -> None:
    """Covers T5.1: package_std + deliver_dropbox charges exist and no duplicates on rerun."""

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    # Minimal repo layout required by orchestrator
    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    # Modules required for this scenario: bigfile_gen (local), package_std, deliver_dropbox
    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_dropbox", repo_root / "modules" / "deliver_dropbox")

    # Tenant + workorder
    tenant_id = "nxlkGI"
    work_order_id = "WOOO0001"
    wo_dir = repo_root / "tenants" / tenant_id / "workorders"
    wo_dir.mkdir(parents=True, exist_ok=True)

    (wo_dir / f"{work_order_id}.yml").write_text(
        textwrap.dedent(
            """\
            tenant_id: nxlkGI
            enabled: true
            mode: PARTIAL_ALLOWED
            artifacts_requested: true
            steps:
              - step_id: s1
                module_id: bigfile_gen
                kind: transform
                inputs:
                  bytes: 128
                  seed: test
                requested_deliverables: []
              - step_id: p2
                module_id: package_std
                kind: packaging
                inputs:
                  bound_outputs:
                    - from_step: s1
                      output_id: big_file
                      as_path: source/big.bin
                    - from_step: s1
                      output_id: report
                      as_path: source/report.json
                requested_deliverables: []
              - step_id: d3
                module_id: deliver_dropbox
                kind: delivery
                inputs:
                  package_zip:
                    from_step: p2
                    output_id: package_zip
                  manifest_json:
                    from_step: p2
                    output_id: manifest_json
                  remote_base_path: /Apps/Platform
                requested_deliverables: []
            """
        ),
        encoding="utf-8",
    )

    # Queue: a simple CSV pointing to the workorder file
    # Orchestrator reads maintenance-state/workorders_index.csv as the queue in dev-github profile.
    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"

    # Seed billing-state with required CSVs
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    profile = load_runtime_profile(repo_root)
    # Force adapters to use the per-test billing_state_dir instead of profile default ".billing-state".
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    # First run
    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items1 = read_csv(billing_state_dir / "transaction_items.csv")

    # Second run (same workorder)
    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items2 = read_csv(billing_state_dir / "transaction_items.csv")

    # No new ledger items should be added on rerun.
    assert len(items2) == len(items1)

    # Ensure we charged for package_std and deliver_dropbox step runs at least once.
    mods = [str(r.get("module_id") or "") for r in items2]
    assert "package_std" in mods
    assert "deliver_dropbox" in mods

    # Ensure no duplicate idempotency keys exist.
    keys = []
    for r in items2:
        meta = str(r.get("metadata_json") or "{}").strip()
        try:
            m = json.loads(meta) if meta else {}
        except Exception:
            m = {}
        k = str(m.get("idempotency_key") or "").strip()
        if k:
            keys.append(k)
    assert len(keys) == len(set(keys))
