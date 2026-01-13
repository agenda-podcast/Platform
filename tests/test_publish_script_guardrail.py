from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def test_publish_artifacts_release_no_publish_mode_smoke(tmp_path: Path) -> None:
    """Guardrail: ensure scripts/publish_artifacts_release.py remains runnable.

    This test runs the real publish script in --no-publish mode against the
    repository's deterministic E2E fixtures (runtime-e2e + .billing-state-e2e).

    If the script regresses (import error, adapter mismatch, attribute error,
    schema drift), pytest will fail and block the change.
    """

    repo_root = Path(__file__).resolve().parents[1]
    script = repo_root / "scripts" / "publish_artifacts_release.py"

    # These directories are shipped with the repo snapshot used for Milestone L.
    src_billing = repo_root / ".billing-state-e2e"
    src_runtime = repo_root / "runtime-e2e"
    assert script.exists(), f"Missing script: {script}"
    assert src_billing.exists(), f"Missing fixture billing-state: {src_billing}"
    assert src_runtime.exists(), f"Missing fixture runtime dir: {src_runtime}"

    billing_state_dir = tmp_path / ".billing-state-e2e"
    runtime_dir = tmp_path / "runtime-e2e"
    dist_dir = tmp_path / "dist_artifacts"

    shutil.copytree(src_billing, billing_state_dir)
    shutil.copytree(src_runtime, runtime_dir)

    env = dict(os.environ)
    env["PLATFORM_OFFLINE"] = "1"

    cmd = [
        sys.executable,
        str(script),
        "--runtime-profile",
        str(repo_root / "config" / "runtime_profile.dev_github.yml"),
        "--billing-state-dir",
        str(billing_state_dir),
        "--runtime-dir",
        str(runtime_dir),
        "--dist-dir",
        str(dist_dir),
        "--since",
        "2026-01-12T13:04:00Z",
        "--no-publish",
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # publish_artifacts_release.py uses exit code 2 to signal that it wrote refunds
    # (for example when a purchased deliverable output is missing). That is not
    # a script regression; the guardrail is that the script remains runnable.
    assert proc.returncode in (0, 2), proc.stdout
    assert dist_dir.exists(), "dist_dir was not created"