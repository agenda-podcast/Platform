from __future__ import annotations

from pathlib import Path
from typing import Any, List

from platform.common.id_policy import validate_id
from platform.utils.csvio import read_csv

from . import core


def _validate_modules(repo_root: Path) -> None:
    modules_dir = repo_root / "modules"
    if not modules_dir.exists():
        core._fail("modules/ directory missing")

    module_ids = set()
    for d in sorted(modules_dir.iterdir(), key=lambda p: p.name):
        if not d.is_dir():
            continue

        mid = d.name.strip()
        try:
            validate_id("module_id", mid, "module_id")
        except Exception as e:
            core._fail(f"Invalid module folder name: {mid!r}: {e}")

        module_ids.add(mid)

        myml = d / "module.yml"
        if not myml.exists():
            core._fail(f"Missing module.yml for module {mid}")

        cfg = core._read_yaml(myml)
        declared = str(cfg.get("module_id", "")).strip()
        if declared and declared != mid:
            core._fail(f"module.yml module_id mismatch for {mid}: declared={declared!r}")

        # Dependencies are not allowed at the module layer.
        # All wiring must be expressed in workorders via steps and bindings.
        if "depends_on" in cfg and (cfg.get("depends_on") not in (None, [], "")):
            core._fail(f"Module {mid} defines depends_on, but module dependencies are not supported")

        ports = cfg.get("ports") or {}
        if not isinstance(ports, dict):
            core._fail(f"Module {mid}: ports must be an object")

        p_in = ports.get("inputs") or {}
        p_out = ports.get("outputs") or {}
        if not isinstance(p_in, dict) or not isinstance(p_out, dict):
            core._fail(f"Module {mid}: ports.inputs and ports.outputs must be objects")

        in_port = p_in.get("port") or []
        in_limited = p_in.get("limited_port") or []
        out_port = p_out.get("port") or []
        out_limited = p_out.get("limited_port") or []

        if not all(isinstance(x, list) for x in (in_port, in_limited, out_port, out_limited)):
            core._fail(f"Module {mid}: ports.*.port and ports.*.limited_port must be lists")

        def _validate_port_list(lst: List[Any], kind: str) -> None:
            seen = set()
            for p in lst:
                if not isinstance(p, dict):
                    core._fail(f"Module {mid}: invalid {kind} port (expected object)")
                pid = str(p.get("id", "")).strip()
                if not pid:
                    core._fail(f"Module {mid}: {kind} port missing id")
                if pid in seen:
                    core._fail(f"Module {mid}: duplicate {kind} port id {pid!r}")
                seen.add(pid)

                # For tenant-visible output ports, require non-empty path.
                if kind.startswith("outputs.port"):
                    path = str(p.get("path", "")).lstrip("/").strip()
                    if not path:
                        core._fail(
                            f"Module {mid}: tenant-visible output port {pid!r} must define non-empty path"
                        )

        _validate_port_list(in_port, "inputs.port")
        _validate_port_list(in_limited, "inputs.limited_port")
        _validate_port_list(out_port, "outputs.port")
        _validate_port_list(out_limited, "outputs.limited_port")

    # platform/modules/modules.csv must match folders
    pm = repo_root / "platform" / "modules" / "modules.csv"
    core._assert_exact_header(pm, ["module_id", "module_name", "version", "folder", "entrypoint", "description"])

    rows = read_csv(pm)
    for r in rows:
        mid = str(r.get("module_id", "")).strip()
        if not mid:
            continue

        validate_id("module_id", mid, "platform/modules/modules.csv module_id")

        folder = str(r.get("folder", "")).strip()
        if folder and folder != mid:
            core._fail(f"modules.csv folder mismatch: module_id={mid!r} folder={folder!r}")
        if mid not in module_ids:
            core._fail(f"modules.csv references missing module folder: {mid!r}")

    core._ok("Modules: folder IDs + module.yml + modules.csv OK")
