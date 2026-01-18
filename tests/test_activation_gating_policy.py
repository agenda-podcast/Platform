from __future__ import annotations

import tempfile
from pathlib import Path

import yaml

from _testutil import ensure_repo_on_path


def _write_workorder(tmp_path: Path, obj: dict) -> Path:
    p = tmp_path / "workorder.yml"
    p.write_text(yaml.safe_dump(obj, sort_keys=False), encoding="utf-8")
    return p


def test_activation_gating_enabled_artifacts_requires_packaging_and_delivery() -> None:
    ensure_repo_on_path()


    repo_root = Path(__file__).resolve().parents[1]
    rules = load_rules_table(repo_root)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # Missing packaging
        wo = {
            "tenant_id": "nxlkGI",
            "enabled": True,
            "artifacts_requested": True,
            "steps": [
                {"step_id": "d1", "module_id": "deliver_dropbox", "kind": "delivery", "inputs": {}},
            ],
        }
        p = _write_workorder(tmp, wo)
        try:
            validate_workorder(repo_root=repo_root, workorder_path=p, module_rules_by_id=rules)
            assert False, "expected failure"
        except ConsistencyValidationError as e:
            assert "missing packaging step" in str(e)

        # Missing delivery
        wo = {
            "tenant_id": "nxlkGI",
            "enabled": True,
            "artifacts_requested": True,
            "steps": [
                {"step_id": "p1", "module_id": "package_std", "kind": "packaging", "inputs": {"bound_outputs": []}},
            ],
        }
        p = _write_workorder(tmp, wo)
        try:
            validate_workorder(repo_root=repo_root, workorder_path=p, module_rules_by_id=rules)
            assert False, "expected failure"
        except ConsistencyValidationError as e:
            assert "missing delivery step" in str(e)


def test_activation_gating_wrong_order_delivery_before_packaging() -> None:
    ensure_repo_on_path()


    repo_root = Path(__file__).resolve().parents[1]
    rules = load_rules_table(repo_root)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        wo = {
            "tenant_id": "nxlkGI",
            "enabled": True,
            "artifacts_requested": True,
            "steps": [
                {"step_id": "d1", "module_id": "deliver_dropbox", "kind": "delivery", "inputs": {}},
                {"step_id": "p2", "module_id": "package_std", "kind": "packaging", "inputs": {"bound_outputs": []}},
            ],
        }
        p = _write_workorder(tmp, wo)
        try:
            validate_workorder(repo_root=repo_root, workorder_path=p, module_rules_by_id=rules)
            assert False, "expected failure"
        except ConsistencyValidationError as e:
            assert "wrong order (delivery before packaging)" in str(e)


def test_activation_gating_disabled_is_warning_only() -> None:
    ensure_repo_on_path()


    repo_root = Path(__file__).resolve().parents[1]
    rules = load_rules_table(repo_root)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        wo = {
            "tenant_id": "nxlkGI",
            "enabled": False,
            "artifacts_requested": True,
            "steps": [
                {"step_id": "d1", "module_id": "deliver_dropbox", "kind": "delivery", "inputs": {}},
            ],
        }
        p = _write_workorder(tmp, wo)
        res = validate_workorder_preflight(repo_root=repo_root, workorder_path=p, module_rules_by_id=rules)
        warns = "\n".join(res.get("warnings") or [])
        assert "missing packaging step" in warns


def test_activation_gating_packaging_requires_delivery_even_without_artifacts_requested() -> None:
    ensure_repo_on_path()


    repo_root = Path(__file__).resolve().parents[1]
    rules = load_rules_table(repo_root)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        wo = {
            "tenant_id": "nxlkGI",
            "enabled": True,
            "artifacts_requested": False,
            "steps": [
                {"step_id": "p1", "module_id": "package_std", "kind": "packaging", "inputs": {"bound_outputs": []}},
            ],
        }
        pth = _write_workorder(tmp, wo)
        try:
            validate_workorder(repo_root=repo_root, workorder_path=pth, module_rules_by_id=rules)
            assert False, "expected failure"
        except ConsistencyValidationError as e:
            assert "missing delivery step" in str(e)


def test_activation_gating_packaging_requires_delivery_disabled_is_warning_only() -> None:
    ensure_repo_on_path()


    repo_root = Path(__file__).resolve().parents[1]
    rules = load_rules_table(repo_root)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        wo = {
            "tenant_id": "nxlkGI",
            "enabled": False,
            "artifacts_requested": False,
            "steps": [
                {"step_id": "p1", "module_id": "package_std", "kind": "packaging", "inputs": {"bound_outputs": []}},
            ],
        }
        pth = _write_workorder(tmp, wo)
        res = validate_workorder_preflight(repo_root=repo_root, workorder_path=pth, module_rules_by_id=rules)
        warns = "\n".join(res.get("warnings") or [])
        assert "missing delivery step" in warns
