from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path

from platform.infra.adapters.exec_local import LocalExecutionBackend
from platform.infra.adapters.registry_repo import RepoModuleRegistry
from platform.infra.adapters.runstate_csv import CsvRunStateStore
from platform.infra.models import StepSpec, WorkorderSpec
from platform.orchestration.module_exec import execute_module_runner


def _repo_root() -> Path:
    # tests/ is at repo_root/tests
    return Path(__file__).resolve().parents[1]


def test_package_std_determinism_and_metadata(tmp_path: Path) -> None:
    repo_root = _repo_root()
    registry = RepoModuleRegistry(repo_root)
    run_state = CsvRunStateStore(tmp_path / "runstate")
    backend = LocalExecutionBackend(repo_root=repo_root, registry=registry, run_state=run_state)

    tenant_id = "AbC123"
    work_order_id = "Wk123456"

    workorder = WorkorderSpec(tenant_id=tenant_id, work_order_id=work_order_id, steps=[])

    # Step 1: create deterministic outputs.
    s1_out = tmp_path / "runs" / tenant_id / work_order_id / "s1"
    s1_out.mkdir(parents=True)
    step1 = StepSpec(
        step_id="s1",
        module_id="U2T",
        inputs={"topic": "determinism test"},
        deliverables=["tenant_outputs"],
        metadata={"idempotency_key": "step1"},
    )
    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step1, outputs_dir=s1_out)

    # Step 2: package bound outputs using output bindings.
    s2_out = tmp_path / "runs" / tenant_id / work_order_id / "s2"
    s2_out.mkdir(parents=True)
    step2 = StepSpec(
        step_id="s2",
        module_id="package_std",
        inputs={
            "bound_outputs": [
                {"from_step": "s1", "output_id": "report", "as_path": "reports/report.json"},
                {"from_step": "s1", "output_id": "source_text", "as_path": "texts/source_text.txt"},
            ]
        },
        deliverables=["package_artifacts"],
        metadata={"idempotency_key": "pkg"},
    )

    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step2, outputs_dir=s2_out)

    package_zip = s2_out / "package.zip"
    manifest_json = s2_out / "manifest.json"
    manifest_csv = s2_out / "manifest.csv"

    assert package_zip.exists()
    assert manifest_json.exists()
    assert manifest_csv.exists()

    # Validate manifest content correctness.
    mj = json.loads(manifest_json.read_text(encoding="utf-8"))
    assert mj.get("module_id") == "package_std"
    files = mj.get("files")
    assert isinstance(files, list) and len(files) == 2
    by_dest = {f.get("dest_path"): f for f in files}
    assert set(by_dest.keys()) == {"reports/report.json", "texts/source_text.txt"}
    for dest_path, row in by_dest.items():
        assert int(row.get("bytes") or 0) > 0
        assert str(row.get("sha256") or "")
        assert str(row.get("content_type") or "")
        assert str(row.get("source_step_id") or "") == "s1"
        assert str(row.get("source_module_id") or "") == "U2T"
    assert by_dest["reports/report.json"]["content_type"] == "application/json"
    assert by_dest["texts/source_text.txt"]["content_type"] == "text/plain"

    with manifest_csv.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    csv_dests = {r.get("dest_path") for r in rows}
    assert csv_dests == {"reports/report.json", "texts/source_text.txt"}

    # Validate zip contains exactly the manifest files plus staged files.
    with zipfile.ZipFile(package_zip) as zf:
        names = sorted(zf.namelist())
    assert names == sorted(["manifest.csv", "manifest.json", "reports/report.json", "texts/source_text.txt"])

    first_zip_bytes = package_zip.read_bytes()
    first_manifest_json = manifest_json.read_text(encoding="utf-8")
    first_manifest_csv = manifest_csv.read_text(encoding="utf-8")

    # Re-run packaging with the same inputs and same outputs dir.
    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step2, outputs_dir=s2_out)

    assert package_zip.read_bytes() == first_zip_bytes
    assert manifest_json.read_text(encoding="utf-8") == first_manifest_json
    assert manifest_csv.read_text(encoding="utf-8") == first_manifest_csv

    # Large-file metadata: bytes and sha are recorded for package_zip without re-reading.
    rec_zip = run_state.get_output(tenant_id, work_order_id, "s2", "package_zip")
    assert rec_zip.bytes == package_zip.stat().st_size
    assert rec_zip.sha256
    assert rec_zip.content_type == "application/zip"

    rec_mj = run_state.get_output(tenant_id, work_order_id, "s2", "manifest_json")
    assert rec_mj.bytes == manifest_json.stat().st_size
    assert rec_mj.sha256
    assert rec_mj.content_type == "application/json"

    rec_mc = run_state.get_output(tenant_id, work_order_id, "s2", "manifest_csv")
    assert rec_mc.bytes == manifest_csv.stat().st_size
    assert rec_mc.sha256
    assert rec_mc.content_type == "text/csv"


def test_runstate_idempotency_for_packaging_step(tmp_path: Path) -> None:
    run_state = CsvRunStateStore(tmp_path / "runstate")

    tenant_id = "AbC123"
    work_order_id = "Wk123456"

    a = run_state.create_step_run(
        tenant_id=tenant_id,
        work_order_id=work_order_id,
        step_id="s2",
        module_id="package_std",
        idempotency_key="pkg",
        outputs_dir=tmp_path / "o",
        metadata={"x": 1},
    )
    b = run_state.create_step_run(
        tenant_id=tenant_id,
        work_order_id=work_order_id,
        step_id="s2",
        module_id="package_std",
        idempotency_key="pkg",
        outputs_dir=tmp_path / "o2",
        metadata={"x": 2},
    )
    assert a.module_run_id == b.module_run_id



def test_package_std_missing_bound_outputs_fails_with_package_failed(tmp_path: Path) -> None:
    repo_root = _repo_root()
    registry = RepoModuleRegistry(repo_root)
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Bypass binding resolution and simulate a missing source file referenced at runtime.
    params = {
        "tenant_id": "AbC123",
        "work_order_id": "Wk123456",
        "module_run_id": "MR000000",
        "inputs": {
            "bound_outputs": [
                {
                    "step_id": "s_missing",
                    "module_id": "U2T",
                    "output_id": "report",
                    "uri": "file:///definitely/does/not/exist/report.json",
                    "content_type": "application/json",
                    "as_path": "reports/report.json",
                }
            ]
        },
        "_platform": {"step_id": "s_pkg", "module_id": "package_std", "run_id": "MR000000"},
    }

    result = execute_module_runner(module_path=registry.module_path("package_std"), params=params, outputs_dir=out_dir, env=None)
    assert str(result.get("status") or "").upper() == "FAILED"
    assert result.get("reason_slug") == "package_failed"

    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    missing = report.get("missing_outputs")
    assert missing == [{"step_id": "s_missing", "output_id": "report"}]
