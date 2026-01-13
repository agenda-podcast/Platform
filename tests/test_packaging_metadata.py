from __future__ import annotations

import sys
from pathlib import Path

# The repository package name `platform` collides with Python's stdlib module `platform`.
# If the stdlib module was imported earlier by the test runner or other dependencies,
# remove it so we can import the repository package.
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

from tests.test_delivery_modules import _build_packaged_inputs


def test_packaging_outputs_record_metadata(tmp_path: Path) -> None:
    _, run_state, workorder, _ = _build_packaged_inputs(tmp_path)

    pkg = run_state.get_output(
        tenant_id=workorder.tenant_id,
        work_order_id=workorder.work_order_id,
        step_id="s2",
        output_id="package_zip",
    )
    assert int(pkg.bytes) > 0
    assert isinstance(pkg.sha256, str) and len(pkg.sha256) == 64
    assert pkg.content_type == "application/zip"

    mj = run_state.get_output(
        tenant_id=workorder.tenant_id,
        work_order_id=workorder.work_order_id,
        step_id="s2",
        output_id="manifest_json",
    )
    assert int(mj.bytes) > 0
    assert isinstance(mj.sha256, str) and len(mj.sha256) == 64
    assert mj.content_type == "application/json"

    mc = run_state.get_output(
        tenant_id=workorder.tenant_id,
        work_order_id=workorder.work_order_id,
        step_id="s2",
        output_id="manifest_csv",
    )
    assert int(mc.bytes) > 0
    assert isinstance(mc.sha256, str) and len(mc.sha256) == 64
    assert mc.content_type == "text/csv"
