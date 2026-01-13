from __future__ import annotations

import argparse
import csv
import json
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass(frozen=True)
class StepInfo:
    step_id: str
    module_id: str
    kind: str
    enabled: bool = True


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(p: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_workorder(repo_root: Path, tenant_id: str, work_order_id: str) -> Tuple[bool, List[StepInfo]]:
    wo_path = repo_root / "tenants" / tenant_id / "workorders" / f"{work_order_id}.yml"
    if not wo_path.exists():
        raise AssertionError(f"workorder yaml not found: {wo_path}")

    wo = yaml.safe_load(wo_path.read_text(encoding="utf-8")) or {}
    artifacts_requested = bool(wo.get("artifacts_requested", False))

    steps: List[StepInfo] = []
    for raw in (wo.get("steps") or []):
        if not isinstance(raw, dict):
            continue
        step_id = str(raw.get("step_id") or "").strip()
        module_id = str(raw.get("module_id") or "").strip()
        kind = str(raw.get("kind") or "").strip()
        enabled = bool(raw.get("enabled", True))
        if not step_id or not module_id:
            continue
        if not kind:
            # Fallback to module.yml kind if workorder kind is missing.
            myml = repo_root / "modules" / module_id / "module.yml"
            if myml.exists():
                try:
                    m = yaml.safe_load(myml.read_text(encoding="utf-8")) or {}
                    kind = str(m.get("kind") or "").strip()
                except Exception:
                    kind = ""
        steps.append(StepInfo(step_id=step_id, module_id=module_id, kind=kind, enabled=enabled))
    return artifacts_requested, steps


def _load_module_runs(module_runs_log: Path) -> List[Dict[str, Any]]:
    if not module_runs_log.exists():
        raise AssertionError(f"module_runs_log.csv not found: {module_runs_log}")

    with module_runs_log.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return list(r)


def _get_step_id_from_meta(meta_json: str) -> str:
    try:
        m = json.loads(meta_json or "{}")
    except Exception:
        m = {}
    return str(m.get("step_id") or "").strip()


def _get_outputs_dir_from_meta(meta_json: str, output_ref: str) -> str:
    try:
        m = json.loads(meta_json or "{}")
    except Exception:
        m = {}
    od = str(m.get("outputs_dir") or "").strip()
    if od:
        return od
    return str(output_ref or "").strip()


def _pick_latest_completed(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Prefer ended_at if present, else created_at.
    best: Optional[Dict[str, Any]] = None
    best_key = ""
    for row in runs:
        ended = str(row.get("ended_at") or "").strip()
        created = str(row.get("created_at") or "").strip()
        key = ended or created
        if not best or key >= best_key:
            best = row
            best_key = key
    if not best:
        raise AssertionError("no runs to pick from")
    return best


def _assert_manifest_and_zip(outputs_dir: Path) -> None:
    zip_path = outputs_dir / "package.zip"
    mj_path = outputs_dir / "manifest.json"
    mc_path = outputs_dir / "manifest.csv"

    assert zip_path.exists(), f"missing package.zip: {zip_path}"
    assert mj_path.exists(), f"missing manifest.json: {mj_path}"
    assert mc_path.exists(), f"missing manifest.csv: {mc_path}"

    manifest = _read_json(mj_path)
    files = manifest.get("files")
    assert isinstance(files, list), "manifest.json missing files list"
    assert files, "manifest.json files list is empty"

    # Validate JSON list structure and deterministic ordering.
    keys: List[Tuple[str, str]] = []
    json_map: Dict[str, Tuple[int, str]] = {}
    for i, row in enumerate(files):
        assert isinstance(row, dict), f"manifest.json files[{i}] must be object"
        dest = str(row.get("dest_path") or "").strip()
        sha = str(row.get("sha256") or "").strip()
        b = int(row.get("bytes") or 0)

        assert dest, f"manifest.json files[{i}] missing dest_path"
        assert not dest.startswith("/"), f"manifest.json files[{i}] dest_path must be relative: {dest}"
        assert ".." not in dest.split("/"), f"manifest.json files[{i}] dest_path must not traverse: {dest}"

        assert b > 0, f"manifest.json files[{i}] bytes must be > 0"
        assert sha, f"manifest.json files[{i}] missing sha256"

        keys.append((dest, sha))
        if dest in json_map:
            raise AssertionError(f"manifest.json contains duplicate dest_path: {dest}")
        json_map[dest] = (b, sha)

    assert keys == sorted(keys), "manifest.json files list is not in deterministic order"

    # Parse CSV and compare to JSON list exactly (same set and consistent bytes/hashes).
    with mc_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        csv_rows = list(r)

    csv_map: Dict[str, Tuple[int, str]] = {}
    for i, crow in enumerate(csv_rows):
        dest_c = str(crow.get("dest_path") or "").strip()
        assert dest_c, f"manifest.csv row {i} missing dest_path"
        if dest_c in csv_map:
            raise AssertionError(f"manifest.csv contains duplicate dest_path: {dest_c}")
        csv_map[dest_c] = (int(crow.get("bytes") or 0), str(crow.get("sha256") or "").strip())

    assert set(csv_map.keys()) == set(json_map.keys()), "manifest.csv dest_path set does not match manifest.json"
    for dest, (b, sha) in sorted(json_map.items(), key=lambda kv: kv[0]):
        cb, csha = csv_map.get(dest, (0, ""))
        assert int(cb) == int(b), f"manifest mismatch for {dest}: bytes differs"
        assert str(csha) == str(sha), f"manifest mismatch for {dest}: sha256 differs"

    # Validate zip contains all files declared in manifest plus manifest files.
    with zipfile.ZipFile(zip_path, "r") as zf:
        names_in_order = [zi.filename for zi in zf.infolist()]
        names_set = set(names_in_order)

        assert "manifest.json" in names_set, "zip missing manifest.json"
        assert "manifest.csv" in names_set, "zip missing manifest.csv"

        for dest in json_map.keys():
            assert dest in names_set, f"zip missing declared file: {dest}"

        # Deterministic ordering: zip entries should be sorted by name.
        assert names_in_order == sorted(names_in_order), "zip entry order is not deterministic (not sorted)"


def _assert_packaging_determinism_across_reruns(packaging_runs: List[Dict[str, Any]]) -> None:
    if len(packaging_runs) < 2:
        return

    ordered = sorted(
        packaging_runs,
        key=lambda r: str(r.get("ended_at") or r.get("created_at") or "").strip(),
    )
    r1 = ordered[-2]
    r2 = ordered[-1]

    od1 = Path(_get_outputs_dir_from_meta(str(r1.get("metadata_json") or ""), str(r1.get("output_ref") or "")))
    od2 = Path(_get_outputs_dir_from_meta(str(r2.get("metadata_json") or ""), str(r2.get("output_ref") or "")))

    z1 = od1 / "package.zip"
    z2 = od2 / "package.zip"
    m1 = od1 / "manifest.json"
    m2 = od2 / "manifest.json"
    c1 = od1 / "manifest.csv"
    c2 = od2 / "manifest.csv"

    assert z1.exists() and z2.exists(), "determinism check requires both package.zip files to exist"
    assert m1.exists() and m2.exists(), "determinism check requires both manifest.json files to exist"
    assert c1.exists() and c2.exists(), "determinism check requires both manifest.csv files to exist"

    sha_z1 = _sha256_file(z1)
    sha_z2 = _sha256_file(z2)
    assert sha_z1 == sha_z2, "package.zip sha256 differs across reruns (packaging is not deterministic)"

    mj1 = json.loads(m1.read_text(encoding="utf-8"))
    mj2 = json.loads(m2.read_text(encoding="utf-8"))
    assert mj1.get("files") == mj2.get("files"), "manifest.json file list differs across reruns"

    csv1 = c1.read_text(encoding="utf-8")
    csv2 = c2.read_text(encoding="utf-8")
    assert csv1 == csv2, "manifest.csv differs across reruns"

    with zipfile.ZipFile(z1, "r") as a, zipfile.ZipFile(z2, "r") as b:
        n1 = [zi.filename for zi in a.infolist()]
        n2 = [zi.filename for zi in b.infolist()]
        assert n1 == n2, "zip entry ordering differs across reruns"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--billing-state-dir", required=True)
    ap.add_argument("--runtime-dir", required=True)
    ap.add_argument("--tenant-id", required=True)
    ap.add_argument("--work-order-id", required=True)
    ap.add_argument("--since", default="")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    artifacts_requested, steps = _parse_workorder(repo_root, args.tenant_id, args.work_order_id)

    enabled_steps = [s for s in steps if s.enabled]
    packaging_steps = [s for s in enabled_steps if s.kind == "packaging"]
    delivery_steps = [s for s in enabled_steps if s.kind == "delivery"]

    if artifacts_requested:
        assert packaging_steps, "artifacts_requested=true requires at least one enabled packaging step"
        assert delivery_steps, "artifacts_requested=true requires at least one enabled delivery step"

    module_runs_log = Path(args.billing_state_dir) / "module_runs_log.csv"
    runs_all = _load_module_runs(module_runs_log)

    # Index COMPLETED runs by (step_id, module_id) using metadata_json.step_id and row.module_id.
    runs_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in runs_all:
        if str(row.get("tenant_id") or "").strip() != args.tenant_id:
            continue
        if str(row.get("work_order_id") or "").strip() != args.work_order_id:
            continue
        status = str(row.get("status") or "").strip().upper()
        if status != "COMPLETED":
            continue
        sid = _get_step_id_from_meta(str(row.get("metadata_json") or ""))
        mid = str(row.get("module_id") or "").strip()
        if not sid or not mid:
            continue
        runs_by_key.setdefault((sid, mid), []).append(row)

    # Packaging must have completed runs for each enabled packaging step.
    for s in packaging_steps:
        k = (s.step_id, s.module_id)
        assert k in runs_by_key, f"missing COMPLETED module run for packaging step: {s.step_id} ({s.module_id})"
        latest = _pick_latest_completed(runs_by_key[k])
        outputs_dir = _get_outputs_dir_from_meta(str(latest.get("metadata_json") or ""), str(latest.get("output_ref") or ""))
        assert outputs_dir, f"packaging run missing outputs_dir/output_ref for step {s.step_id}"
        _assert_manifest_and_zip(Path(outputs_dir))
        _assert_packaging_determinism_across_reruns(runs_by_key[k])

    # Delivery rules:
    # - if artifacts_requested=true: every enabled delivery step must have a COMPLETED run
    # - else if a packaging step exists: at least one enabled delivery step must have a COMPLETED run
    if artifacts_requested:
        for s in delivery_steps:
            k = (s.step_id, s.module_id)
            assert k in runs_by_key, f"missing COMPLETED module run for delivery step: {s.step_id} ({s.module_id})"
    else:
        if packaging_steps:
            ok = False
            for s in delivery_steps:
                if (s.step_id, s.module_id) in runs_by_key:
                    ok = True
                    break
            assert ok, "workorder includes a packaging step so it must include at least one delivery step that ran (COMPLETED)"

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
