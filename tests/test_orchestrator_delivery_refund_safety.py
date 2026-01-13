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


def test_delivery_failure_does_not_refund_without_verified_non_delivery(tmp_path: Path) -> None:
    """T3.4: refund only after verified non-delivery.

    deliver_email fails when recipient_email is missing. That failure is not a verified
    non-delivery (we cannot prove the mail was not sent in the general case), so the
    orchestrator must not post REFUND rows.
    """

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_email", repo_root / "modules" / "deliver_email")

    tenant_id = "nxlkGI"
    work_order_id = "WOOO0002"
    wo_dir = repo_root / "tenants" / tenant_id / "workorders"
    wo_dir.mkdir(parents=True, exist_ok=True)

    # Intentionally omit recipient_email to force a module failure.
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
                module_id: deliver_email
                kind: delivery
                inputs:
                  package_zip:
                    from_step: p2
                    output_id: package_zip
                  manifest_json:
                    from_step: p2
                    output_id: manifest_json
                requested_deliverables: []
            """
        ),
        encoding="utf-8",
    )

    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    profile = load_runtime_profile(repo_root)
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)

    tx_items = read_csv(billing_state_dir / "transaction_items.csv")
    refunds = [r for r in tx_items if str(r.get("type") or "") == "REFUND"]
    assert refunds == []


def test_delivery_size_cap_refunds_when_deterministic_non_delivery(tmp_path: Path) -> None:
    """deliver_email: package_too_large_for_email is deterministic and should refund."""

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_email", repo_root / "modules" / "deliver_email")

    tenant_id = "nxlkGI"
    work_order_id = "WOOO0003"
    wo_dir = repo_root / "tenants" / tenant_id / "workorders"
    wo_dir.mkdir(parents=True, exist_ok=True)

    # Generate a file large enough to exceed the 19.9MB cap.
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
                  bytes: 21000000
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
                module_id: deliver_email
                kind: delivery
                inputs:
                  package_zip:
                    from_step: p2
                    output_id: package_zip
                  manifest_json:
                    from_step: p2
                    output_id: manifest_json
                  recipient_email: dev-null@example.com
                requested_deliverables: []
            """
        ),
        encoding="utf-8",
    )

    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    profile = load_runtime_profile(repo_root)
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)

    tx_items = read_csv(billing_state_dir / "transaction_items.csv")
    refunds = [r for r in tx_items if str(r.get("type") or "") == "REFUND" and str(r.get("module_id") or "") == "deliver_email"]
    assert refunds != []


def test_delivery_evidence_item_written_and_idempotent(tmp_path: Path) -> None:
    """T5.3: include delivery evidence in ledger metadata and keep it idempotent."""

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_dropbox", repo_root / "modules" / "deliver_dropbox")

    tenant_id = "nxlkGI"
    work_order_id = "WOOO0004"
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

    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    profile = load_runtime_profile(repo_root)
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items1 = read_csv(billing_state_dir / "transaction_items.csv")

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)
    items2 = read_csv(billing_state_dir / "transaction_items.csv")
    assert len(items2) == len(items1)

    ev = []
    for r in items2:
        if str(r.get("module_id") or "") != "deliver_dropbox":
            continue
        if str(r.get("deliverable_id") or "") != "__delivery_evidence__":
            continue
        ev.append(r)
    assert len(ev) == 1

    meta = str(ev[0].get("metadata_json") or "{}").strip()
    m = json.loads(meta) if meta else {}
    assert str(m.get("provider") or "") == "dropbox"
    assert str(m.get("remote_path") or "").startswith("/")
    assert str(m.get("verification_status") or "") != ""
    assert int(m.get("bytes") or 0) > 0


def test_delivery_transient_error_verified_succeeds_no_refund(tmp_path: Path, monkeypatch) -> None:
    """deliver_dropbox: transient error after upload should verify and succeed, producing no refunds."""

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_dropbox", repo_root / "modules" / "deliver_dropbox")

    tenant_id = "nxlkGI"
    work_order_id = "WOOO0005"
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
                requested_deliverables: []
            """
        ),
        encoding="utf-8",
    )

    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    # Simulate an upload timeout after the file is written.
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("DROPBOX_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT", "1")
    monkeypatch.delenv("DROPBOX_DEV_STUB_SIMULATE_METADATA_ERROR", raising=False)

    profile = load_runtime_profile(repo_root)
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)

    tx_items = read_csv(billing_state_dir / "transaction_items.csv")
    refunds = [r for r in tx_items if str(r.get("type") or "") == "REFUND" and str(r.get("module_id") or "") == "deliver_dropbox"]
    assert refunds == []


def test_delivery_exception_cannot_verify_no_refund(tmp_path: Path, monkeypatch) -> None:
    """deliver_dropbox: if an exception occurs and we cannot verify remote state, do not refund."""

    ensure_repo_on_path()

    from platform.infra.config import load_runtime_profile
    from platform.infra.factory import build_infra
    from platform.orchestration.orchestrator import run_orchestrator
    from platform.utils.csvio import read_csv

    repo_src = Path(__file__).resolve().parents[1]
    repo_root = tmp_path / "repo"

    _copy_tree(repo_src / "platform", repo_root / "platform")
    _copy_tree(repo_src / "config", repo_root / "config")
    _copy_tree(repo_src / "maintenance-state", repo_root / "maintenance-state")
    _copy_tree(repo_src / "billing-state-seed", repo_root / "billing-state-seed")

    _copy_tree(repo_src / "modules" / "bigfile_gen", repo_root / "modules" / "bigfile_gen")
    _copy_tree(repo_src / "modules" / "package_std", repo_root / "modules" / "package_std")
    _copy_tree(repo_src / "modules" / "deliver_dropbox", repo_root / "modules" / "deliver_dropbox")

    tenant_id = "nxlkGI"
    work_order_id = "WOOO0006"
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
                requested_deliverables: []
            """
        ),
        encoding="utf-8",
    )

    idx = repo_root / "maintenance-state" / "workorders_index.csv"
    idx.write_text(
        "tenant_id,work_order_id,enabled,schedule_cron,title,notes,path\n"
        + f"{tenant_id},{work_order_id},true,,,test,tenants/{tenant_id}/workorders/{work_order_id}.yml\n",
        encoding="utf-8",
    )

    billing_state_dir = tmp_path / "billing"
    runtime_dir = tmp_path / "runtime"
    _copy_tree(repo_root / "billing-state-seed", billing_state_dir)

    # Force failure: upload raises transient error (timeout), and metadata checks also fail.
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("DROPBOX_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT", "1")
    monkeypatch.setenv("DROPBOX_DEV_STUB_SIMULATE_METADATA_ERROR", "1")

    profile = load_runtime_profile(repo_root)
    profile.adapters["run_state_store"].settings["billing_state_dir"] = str(billing_state_dir)
    profile.adapters["ledger_writer"].settings["billing_state_dir"] = str(billing_state_dir)
    infra = build_infra(repo_root=repo_root, profile=profile, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir)

    run_orchestrator(repo_root=repo_root, billing_state_dir=billing_state_dir, runtime_dir=runtime_dir, infra=infra)

    tx_items = read_csv(billing_state_dir / "transaction_items.csv")
    refunds = [r for r in tx_items if str(r.get("type") or "") == "REFUND" and str(r.get("module_id") or "") == "deliver_dropbox"]
    assert refunds == []
