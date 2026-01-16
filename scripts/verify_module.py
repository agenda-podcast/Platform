from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, NoReturn, Tuple

import yaml


def _fail(msg: str, code: int = 2) -> NoReturn:
    print(f"[verify_module][FAIL] {msg}", file=sys.stderr)
    raise SystemExit(code)


def _info(msg: str) -> None:
    print(f"[verify_module] {msg}")


def _read_modules_index(repo_root: Path) -> Dict[str, Dict[str, str]]:
    p = repo_root / "maintenance-state" / "modules_index.csv"
    if not p.exists():
        _fail(f"Missing maintenance-state/modules_index.csv at {p}")
    out: Dict[str, Dict[str, str]] = {}
    with p.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            _fail("modules_index.csv missing headers")
        for row in r:
            mid = str(row.get("module_id") or "").strip()
            if not mid:
                continue
            out[mid] = {k: str(v or "") for k, v in row.items()}
    return out


def _read_yaml(path: Path) -> Dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        _fail(f"Failed to parse YAML: {path}: {e}")
    if not isinstance(data, dict):
        _fail(f"YAML root must be an object: {path}")
    return data


def _resolve_fixture_value(module_dir: Path, obj: Any) -> Any:
    """Resolve any dict of form {fixture: <relpath>, ...} into a bound file object."""
    if isinstance(obj, dict):
        if "fixture" in obj:
            rel = str(obj.get("fixture") or "").strip()
            if not rel:
                _fail("fixture value must be a non-empty relative path")
            pth = (module_dir / rel).resolve()
            if not pth.exists() or not pth.is_file():
                _fail(f"Fixture not found: {pth}")

            # If the fixture dict only declares the fixture path, resolve directly to a string URI.
            # This supports module input shapes that expect a raw string (for example package_std bound_outputs[].uri).
            if set(obj.keys()) == {"fixture"}:
                return f"file://{pth}"

            meta = dict(obj)
            meta.pop("fixture", None)
            meta["uri"] = f"file://{pth}"
            meta.setdefault("path", str(pth))
            return meta
        return {k: _resolve_fixture_value(module_dir, v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_fixture_value(module_dir, v) for v in obj]
    return obj


def _import_run_callable(repo_root: Path, module_dir: Path):
    run_py = module_dir / "src" / "run.py"
    if not run_py.exists():
        _fail(f"Missing module entrypoint: {run_py}")

    # Ensure repository package imports resolve deterministically.
    # The repo defines a top-level package named 'platform' which shadows the stdlib module of the same name.
    repo_path = str(repo_root.resolve())
    src_path = str((module_dir / 'src').resolve())
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    if repo_path not in sys.path:
        sys.path.insert(0, repo_path)
    stdlib_platform = sys.modules.get('platform')
    if stdlib_platform is not None and not hasattr(stdlib_platform, '__path__'):
        del sys.modules['platform']
    spec = importlib.util.spec_from_file_location(f"module_run_{module_dir.name}", run_py)
    if spec is None or spec.loader is None:
        _fail(f"Failed to load module entrypoint: {run_py}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    fn = getattr(mod, "run", None)
    if fn is None or not callable(fn):
        _fail(f"Entrypoint does not define callable run(): {run_py}")
    return fn


def _call_run(fn, params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    try:
        return fn(params=params, outputs_dir=outputs_dir)
    except TypeError:
        # Backward-compatible signature: run(params, outputs_dir)
        return fn(params, outputs_dir)


def _validate_outputs(outputs_dir: Path, expected_status: str, expected_files: List[str], result: Dict[str, Any]) -> None:
    status = str(result.get("status") or "").strip()
    if status != expected_status:
        _fail(f"Status mismatch: expected={expected_status!r} got={status!r}")
    missing: List[str] = []
    for rel in expected_files:
        rp = str(rel or "").strip().replace("\\", "/")
        if not rp:
            continue
        p = outputs_dir / rp
        if not p.exists():
            missing.append(rp)
    if missing:
        _fail(f"Missing expected output files: {missing}")


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Verify a module via its module.yml self_test contract")
    ap.add_argument("--module-id", required=True, help="module_id to verify")
    ap.add_argument("--repo-root", default=".", help="repository root (default: .)")
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    module_id = str(args.module_id).strip()
    if not module_id:
        _fail("--module-id must be non-empty")

    idx = _read_modules_index(repo_root)
    row = idx.get(module_id)
    if not row:
        _fail(f"Unknown module_id in modules_index.csv: {module_id!r}")

    rel_path = str(row.get("path") or "").strip()
    if not rel_path:
        _fail(f"modules_index.csv missing path for module_id={module_id!r}")
    module_dir = (repo_root / rel_path).resolve()
    module_yml = module_dir / "module.yml"
    if not module_yml.exists():
        _fail(f"Missing module.yml: {module_yml}")
    data = _read_yaml(module_yml)

    testing = data.get("testing")
    if not isinstance(testing, dict):
        _fail(f"module.yml missing testing block for module_id={module_id!r}")
    st = testing.get("self_test")
    if not isinstance(st, dict):
        _fail(f"module.yml missing testing.self_test block for module_id={module_id!r}")
    params = st.get("params")
    expect = st.get("expect")
    if not isinstance(params, dict):
        _fail(f"testing.self_test.params must be an object for module_id={module_id!r}")
    if not isinstance(expect, dict):
        _fail(f"testing.self_test.expect must be an object for module_id={module_id!r}")

    expected_status = str(expect.get("status") or "").strip()
    if not expected_status:
        _fail(f"testing.self_test.expect.status is required for module_id={module_id!r}")
    expected_files = expect.get("files")
    if expected_files is None:
        expected_files = []
    if not isinstance(expected_files, list):
        _fail(f"testing.self_test.expect.files must be a list for module_id={module_id!r}")
    expected_files_list = [str(x) for x in expected_files]

    resolved_params = _resolve_fixture_value(module_dir, params)

    with tempfile.TemporaryDirectory(prefix=f"platform-selftest-{module_id}-") as td:
        run_root = Path(td)
        outputs_dir = run_root / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)
        _info(f"module_id={module_id} module_dir={module_dir}")
        _info(f"outputs_dir={outputs_dir}")
        fn = _import_run_callable(repo_root, module_dir)
        result = _call_run(fn, resolved_params, outputs_dir)
        if not isinstance(result, dict):
            _fail(f"Module run() returned non-object result: {type(result)}")
        _info(f"result={result}")
        _validate_outputs(outputs_dir, expected_status, expected_files_list, result)

    _info("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
