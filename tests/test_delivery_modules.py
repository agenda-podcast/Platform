from __future__ import annotations

import json
import sys
from pathlib import Path

# The repository package name `platform` collides with Python's stdlib module `platform`.
# If the stdlib module was imported earlier by the test runner or other dependencies,
# remove it so we can import the repository package.
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

from platform.infra.adapters.exec_local import LocalExecutionBackend
from platform.infra.adapters.registry_repo import RepoModuleRegistry
from platform.infra.adapters.runstate_csv import CsvRunStateStore
from platform.infra.models import StepSpec, WorkorderSpec
from platform.delivery.receipt import validate_delivery_receipt


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _build_packaged_inputs(tmp_path: Path) -> tuple[LocalExecutionBackend, CsvRunStateStore, WorkorderSpec, Path]:
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
        inputs={"topic": "delivery test"},
        deliverables=["tenant_outputs"],
        metadata={"idempotency_key": "step1"},
    )
    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step1, outputs_dir=s1_out)

    # Step 2: package bound outputs.
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

    assert (s2_out / "package.zip").exists()
    return backend, run_state, workorder, s2_out


def test_deliver_email_outbox_stub_success(tmp_path: Path, monkeypatch) -> None:
    backend, _, workorder, s2_out = _build_packaged_inputs(tmp_path)

    # Ensure SMTP is not configured so the module uses dev outbox stub.
    monkeypatch.delenv("EMAIL_SMTP_HOST", raising=False)
    monkeypatch.delenv("EMAIL_SMTP_PORT", raising=False)
    monkeypatch.setenv("DELIVER_EMAIL_DEFAULT_RECIPIENT", "recipient@example.com")

    s3_out = tmp_path / "runs" / workorder.tenant_id / workorder.work_order_id / "s3"
    s3_out.mkdir(parents=True)
    step3 = StepSpec(
        step_id="s3",
        module_id="deliver_email",
        inputs={
            "package_zip": {"from_step": "s2", "output_id": "package_zip"},
            "manifest_json": {"from_step": "s2", "output_id": "manifest_json"},
        },
        deliverables=[],
        metadata={"idempotency_key": "email"},
    )

    backend.execute_step(repo_root=_repo_root(), workorder=workorder, step=step3, outputs_dir=s3_out)

    receipt_path = s3_out / "delivery_receipt.json"
    assert receipt_path.exists()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    validate_delivery_receipt(receipt)
    assert receipt.get("provider") == "outbox_stub"
    assert receipt.get("verification_status") == "written"
    assert receipt.get("message_id")

    rp = Path(str(receipt.get("remote_path") or ""))
    assert rp.exists()


def test_deliver_email_too_large_fails(tmp_path: Path, monkeypatch) -> None:
    repo_root = _repo_root()
    registry = RepoModuleRegistry(repo_root)
    run_state = CsvRunStateStore(tmp_path / "runstate")
    backend = LocalExecutionBackend(repo_root=repo_root, registry=registry, run_state=run_state)

    tenant_id = "AbC123"
    work_order_id = "Wk123456"
    workorder = WorkorderSpec(tenant_id=tenant_id, work_order_id=work_order_id, steps=[])

    # Create a sparse file just over the threshold (no heavy memory usage).
    max_bytes = 20866662
    big_dir = tmp_path / "runs" / tenant_id / work_order_id / "p"
    big_dir.mkdir(parents=True)
    big_zip = big_dir / "package.zip"
    with big_zip.open("wb") as f:
        f.seek(max_bytes)
        f.write(b"0")

    # Provide a direct file path string to the module.
    monkeypatch.setenv("DELIVER_EMAIL_DEFAULT_RECIPIENT", "recipient@example.com")
    monkeypatch.delenv("EMAIL_SMTP_HOST", raising=False)
    monkeypatch.delenv("EMAIL_SMTP_PORT", raising=False)

    out_dir = tmp_path / "runs" / tenant_id / work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_email",
        inputs={"package_zip": str(big_zip), "recipient_email": "recipient@example.com"},
        deliverables=[],
        metadata={"idempotency_key": "email_big"},
    )

    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step, outputs_dir=out_dir)

    report_path = out_dir / "report.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("reason_slug") == "package_too_large_for_email"






def test_deliver_email_threshold_just_under_succeeds(tmp_path: Path, monkeypatch) -> None:
    """Boundary test: bytes == MAX_PACKAGE_BYTES-1 succeeds."""
    repo_root = _repo_root()
    registry = RepoModuleRegistry(repo_root)
    run_state = CsvRunStateStore(tmp_path / "runstate")
    backend = LocalExecutionBackend(repo_root=repo_root, registry=registry, run_state=run_state)

    tenant_id = "AbC123"
    work_order_id = "Wk123456"
    workorder = WorkorderSpec(tenant_id=tenant_id, work_order_id=work_order_id, steps=[])

    max_bytes = 20866662

    pkg_dir = tmp_path / "runs" / tenant_id / work_order_id / "p"
    pkg_dir.mkdir(parents=True)
    tiny_zip = pkg_dir / "package.zip"
    tiny_zip.write_bytes(b"tiny")

    monkeypatch.setenv("DELIVER_EMAIL_DEFAULT_RECIPIENT", "recipient@example.com")
    monkeypatch.delenv("EMAIL_SMTP_HOST", raising=False)
    monkeypatch.delenv("EMAIL_SMTP_PORT", raising=False)

    out_dir = tmp_path / "runs" / tenant_id / work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_email",
        inputs={"package_zip": {"uri": tiny_zip.resolve().as_uri(), "bytes": max_bytes - 1}},
        deliverables=[],
        metadata={"idempotency_key": "email_under"},
    )

    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step, outputs_dir=out_dir)

    receipt_path = out_dir / "delivery_receipt.json"
    assert receipt_path.exists()
def test_deliver_email_threshold_equal_fails_without_hashing(tmp_path: Path, monkeypatch) -> None:
    """Boundary test: bytes == MAX_PACKAGE_BYTES must fail."""
    repo_root = _repo_root()
    registry = RepoModuleRegistry(repo_root)
    run_state = CsvRunStateStore(tmp_path / "runstate")
    backend = LocalExecutionBackend(repo_root=repo_root, registry=registry, run_state=run_state)

    tenant_id = "AbC123"
    work_order_id = "Wk123456"
    workorder = WorkorderSpec(tenant_id=tenant_id, work_order_id=work_order_id, steps=[])

    # Tiny file, but provide bytes hint at exactly the threshold so the module fails before hashing or reading.
    max_bytes = 20866662
    pkg_dir = tmp_path / "runs" / tenant_id / work_order_id / "p"
    pkg_dir.mkdir(parents=True)
    tiny_zip = pkg_dir / "package.zip"
    tiny_zip.write_bytes(b"tiny")

    monkeypatch.setenv("DELIVER_EMAIL_DEFAULT_RECIPIENT", "recipient@example.com")
    monkeypatch.delenv("EMAIL_SMTP_HOST", raising=False)
    monkeypatch.delenv("EMAIL_SMTP_PORT", raising=False)

    out_dir = tmp_path / "runs" / tenant_id / work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_email",
        inputs={"package_zip": {"uri": tiny_zip.resolve().as_uri(), "bytes": max_bytes}},
        deliverables=[],
        metadata={"idempotency_key": "email_boundary"},
    )

    backend.execute_step(repo_root=repo_root, workorder=workorder, step=step, outputs_dir=out_dir)

    report_path = out_dir / "report.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report.get("reason_slug") == "package_too_large_for_email"

def test_deliver_dropbox_dev_stub_success(tmp_path: Path, monkeypatch) -> None:
    backend, _, workorder, _ = _build_packaged_inputs(tmp_path)

    # Ensure real Dropbox is not used.
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("DROPBOX_CREATE_SHARE_LINK", "false")

    out_dir = tmp_path / "runs" / workorder.tenant_id / workorder.work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_dropbox",
        inputs={
            "package_zip": {"from_step": "s2", "output_id": "package_zip"},
            "manifest_json": {"from_step": "s2", "output_id": "manifest_json"},
            "remote_base_path": "/Apps/Platform",
        },
        deliverables=[],
        metadata={"idempotency_key": "dbx"},
    )

    backend.execute_step(repo_root=_repo_root(), workorder=workorder, step=step, outputs_dir=out_dir)

    receipt_path = out_dir / "delivery_receipt.json"
    assert receipt_path.exists()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    validate_delivery_receipt(receipt)
    assert receipt.get("provider") == "dropbox"
    assert receipt.get("verification_status") == "verified"
    remote_path = str(receipt.get("remote_path") or "")
    expected = f"/{workorder.tenant_id}/{workorder.work_order_id}/{receipt['module_run_id']}/s3/package_zip/package.zip"
    assert remote_path == expected

    # Dev stub writes to a stable stub root anchored at the runtime directory (parent of runs/).
    stub_root = tmp_path / "dropbox_stub"
    stored = stub_root / remote_path.lstrip("/")
    assert stored.exists()
    assert stored.stat().st_size == int(receipt["bytes"])

    import hashlib

    h = hashlib.sha256()
    with stored.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    assert h.hexdigest() == str(receipt.get("sha256") or "")


def test_deliver_dropbox_transient_error_verify_loop(tmp_path: Path, monkeypatch) -> None:
    backend, _, workorder, _ = _build_packaged_inputs(tmp_path)

    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)

    # Monkeypatch default_client in the module runner to return a client that
    # writes the file but raises a transient error, forcing the verify loop.
    import importlib.util

    runner_path = _repo_root() / "modules" / "deliver_dropbox" / "src" / "run.py"
    # Ensure sibling helper module (dropbox_client.py) is importable.
    sys.path.insert(0, str(runner_path.parent))
    spec = importlib.util.spec_from_file_location("deliver_dropbox_run", runner_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]

    # Import helper module directly from the src/ directory.
    import dropbox_client as client_mod

    class FlakyClient(client_mod.DropboxDevStubClient):
        def upload_resumable(self, *, local_path: Path, remote_path: str, chunk_bytes: int) -> None:  # type: ignore[override]
            super().upload_resumable(local_path=local_path, remote_path=remote_path, chunk_bytes=chunk_bytes)
            raise client_mod.DropboxTransientError("simulated transient")

    def _patched_default_client(outputs_dir: Path):
        return FlakyClient(outputs_dir / "dropbox_stub")

    monkeypatch.setattr(mod, "default_client", _patched_default_client)

    out_dir = tmp_path / "runs" / workorder.tenant_id / workorder.work_order_id / "s3"
    out_dir.mkdir(parents=True)

    # Execute via LocalExecutionBackend to mirror production sys.path behavior.
    step = StepSpec(
        step_id="s3",
        module_id="deliver_dropbox",
        inputs={
            "package_zip": {"from_step": "s2", "output_id": "package_zip"},
            "remote_base_path": "/Apps/Platform",
        },
        deliverables=[],
        metadata={"idempotency_key": "dbx_flaky"},
    )

    # The backend will load the runner fresh, not the patched module above.
    # So we call the patched runner directly for the semantic test.
    pkg_rec = backend.run_state.get_output(workorder.tenant_id, workorder.work_order_id, "s2", "package_zip")
    params = {
        "tenant_id": workorder.tenant_id,
        "work_order_id": workorder.work_order_id,
        "module_run_id": "MR1",
        "inputs": {"package_zip": pkg_rec.__dict__, "remote_base_path": "/Apps/Platform"},
        "_platform": {"step_id": "s3", "module_id": "deliver_dropbox"},
    }

    res = mod.run(params=params, outputs_dir=out_dir)
    assert res.get("status") == "COMPLETED"
    receipt = json.loads((out_dir / "delivery_receipt.json").read_text(encoding="utf-8"))
    validate_delivery_receipt(receipt)
    assert receipt.get("verification_status") == "verified_after_transient_error"
    assert str(receipt.get("remote_path") or "") == f"/{workorder.tenant_id}/{workorder.work_order_id}/MR1/s3/package_zip/package.zip"


def test_deliver_dropbox_transient_error_but_verified_succeeds(tmp_path: Path, monkeypatch) -> None:
    backend, _, workorder, _ = _build_packaged_inputs(tmp_path)

    # Ensure real Dropbox is not used.
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("DROPBOX_CREATE_SHARE_LINK", "false")

    # Simulate a timeout after the dev stub completes the upload.
    monkeypatch.setenv("DROPBOX_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT", "1")

    out_dir = tmp_path / "runs" / workorder.tenant_id / workorder.work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_dropbox",
        inputs={
            "package_zip": {"from_step": "s2", "output_id": "package_zip"},
            "manifest_json": {"from_step": "s2", "output_id": "manifest_json"},
            "remote_base_path": "/Apps/Platform",
        },
        deliverables=[],
        metadata={"idempotency_key": "dbx_timeout"},
    )

    backend.execute_step(repo_root=_repo_root(), workorder=workorder, step=step, outputs_dir=out_dir)

    receipt_path = out_dir / "delivery_receipt.json"
    assert receipt_path.exists()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    validate_delivery_receipt(receipt)
    assert receipt.get("provider") == "dropbox"
    assert receipt.get("verification_status") == "verified_after_transient_error"

    remote_path = str(receipt.get("remote_path") or "")
    stub_root = tmp_path / "dropbox_stub"
    stored = stub_root / remote_path.lstrip("/")
    assert stored.exists()
    assert stored.stat().st_size == int(receipt["bytes"])


def test_deliver_onedrive_transient_error_but_verified_succeeds(tmp_path: Path, monkeypatch) -> None:
    backend, _, workorder, _ = _build_packaged_inputs(tmp_path)

    # Ensure real OneDrive is not used.
    monkeypatch.delenv("ONEDRIVE_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("ONEDRIVE_CREATE_SHARE_LINK", "false")

    # Simulate a timeout after the dev stub completes the upload.
    monkeypatch.setenv("ONEDRIVE_DEV_STUB_SIMULATE_UPLOAD_TIMEOUT", "1")

    out_dir = tmp_path / "runs" / workorder.tenant_id / workorder.work_order_id / "s3"
    out_dir.mkdir(parents=True)
    step = StepSpec(
        step_id="s3",
        module_id="deliver_onedrive",
        inputs={
            "package_zip": {"from_step": "s2", "output_id": "package_zip"},
            "manifest_json": {"from_step": "s2", "output_id": "manifest_json"},
            "remote_base_path": "/Apps/Platform",
        },
        deliverables=[],
        metadata={"idempotency_key": "od_timeout"},
    )

    backend.execute_step(repo_root=_repo_root(), workorder=workorder, step=step, outputs_dir=out_dir)

    receipt_path = out_dir / "delivery_receipt.json"
    assert receipt_path.exists()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    validate_delivery_receipt(receipt)
    assert receipt.get("provider") == "onedrive"
    assert receipt.get("verification_status") == "verified_after_transient_error"

    remote_path = str(receipt.get("remote_path") or "")
    stub_root = tmp_path / "onedrive_stub"
    stored = stub_root / remote_path.lstrip("/")
    assert stored.exists()
    assert stored.stat().st_size == int(receipt["bytes"])
