"""E2E verification: release_sync integration is import-safe and no-op offline.

This script should be added to your offline CI E2E phase.

It verifies:
- platform.orchestration.release_sync imports without side effects
- maybe_sync_artifacts_to_release() does not raise when GitHub creds are absent
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def main() -> int:
    os.environ.pop("GITHUB_TOKEN", None)
    os.environ.pop("GITHUB_REPOSITORY", None)
    os.environ["PLATFORM_FORCE_RELEASE_SYNC"] = "1"  # force code path past purchase gate

    from platform.orchestration.release_sync import maybe_sync_artifacts_to_release

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        tenants_dir = root / "tenants"
        runtime_dir = root / "runtime"
        (tenants_dir / "t1" / "outputs" / "wo1").mkdir(parents=True, exist_ok=True)
        (tenants_dir / "t1" / "outputs" / "wo1" / "hello.txt").write_text("hi", encoding="utf-8")

        res = maybe_sync_artifacts_to_release(
            tenant_id="t1",
            work_order_id="wo1",
            tenants_dir=tenants_dir,
            runtime_dir=runtime_dir,
            workorder_dict={"purchases": ["artifacts_download"]},
        )

        assert res.ran is False, "Should be a no-op without GitHub creds"
        assert res.skipped_reason, "Expected a skip reason"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
