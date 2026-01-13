from __future__ import annotations

import csv
import json
import os
import re
import zipfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from platform.utils.hashing import sha256_file


MODULE_ID = "package_std"

_INVALID_PATH_RE = re.compile(r"(^/)|(^\\\\)|(^[A-Za-z]:)|(^\\.\\.)|(\\.\\./)|(/\\.\\.)")


def _fail(outputs_dir: Path, reason_slug: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    report = {
        "module_id": MODULE_ID,
        "reason_slug": reason_slug,
        **(payload or {}),
    }
    (outputs_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"status": "FAILED", "reason_slug": reason_slug, "files": ["report.json"], "metadata": report}


def _error(outputs_dir: Path, reason_slug: str, message: str) -> Dict[str, Any]:
    return _fail(outputs_dir, reason_slug, {"message": message})


def _uri_to_path(uri: str) -> Path:
    u = str(uri or "").strip()
    if not u:
        raise ValueError("missing uri")
    if u.startswith("file://"):
        # file:///abs/path
        p = Path(u.replace("file://", "", 1))
        return p
    # Allow plain filesystem paths in dev usage.
    return Path(u)


def _safe_dest_path(p: str) -> str:
    s = str(p or "").replace("\\\\", "/").strip()
    s = s.lstrip("/")
    if not s:
        raise ValueError("empty destination path")
    if _INVALID_PATH_RE.search(s):
        raise ValueError(f"invalid destination path: {p!r}")
    # Normalize redundant segments.
    parts = []
    for seg in s.split("/"):
        seg = seg.strip()
        if not seg or seg == ".":
            continue
        if seg == "..":
            raise ValueError(f"invalid destination path: {p!r}")
        parts.append(seg)
    if not parts:
        raise ValueError("empty destination path")
    return "/".join(parts)


def _default_dest(rec: Dict[str, Any], src_path: Path) -> str:
    step_id = str(rec.get("step_id") or "").strip() or "step"
    output_id = str(rec.get("output_id") or "").strip() or "output"
    name = src_path.name
    return f"{step_id}/{output_id}/{name}"


def _write_manifest_json(path: Path, manifest: Dict[str, Any]) -> None:
    # Deterministic JSON: sorted keys, stable separators.
    txt = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), indent=2)
    path.write_text(txt + "\n", encoding="utf-8")


def _write_manifest_csv(path: Path, files: List[Dict[str, Any]]) -> None:
    headers = [
        "dest_path",
        "bytes",
        "sha256",
        "content_type",
        "source_step_id",
        "source_module_id",
        "source_output_id",
        "source_path",
        "source_uri",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        w.writeheader()
        for row in files:
            out_row = {
                "dest_path": row.get("dest_path", ""),
                "bytes": row.get("bytes", 0),
                "sha256": row.get("sha256", ""),
                "content_type": row.get("content_type", ""),
                "source_step_id": row.get("source_step_id", ""),
                "source_module_id": row.get("source_module_id", ""),
                "source_output_id": row.get("source_output_id", ""),
                "source_path": row.get("source_path", ""),
                "source_uri": row.get("source_uri", ""),
            }
            w.writerow(out_row)


def _zip_deterministic(zip_path: Path, manifest_json: Path, manifest_csv: Path, staged_root: Path, files: List[Dict[str, Any]]) -> None:
    # Deterministic ZIP: stable entry order, stable timestamps, stable permissions.
    fixed_dt = (1980, 1, 1, 0, 0, 0)

    def _zipinfo(name: str) -> zipfile.ZipInfo:
        zi = zipfile.ZipInfo(filename=name, date_time=fixed_dt)
        zi.compress_type = zipfile.ZIP_DEFLATED
        zi.create_system = 3
        zi.external_attr = (0o644 & 0xFFFF) << 16
        return zi

    entries: List[Tuple[str, Path]] = []
    entries.append(("manifest.json", manifest_json))
    entries.append(("manifest.csv", manifest_csv))
    for row in files:
        dp = str(row.get("dest_path") or "").replace("\\\\", "/")
        sp = staged_root / dp
        entries.append((dp, sp))

    # Ensure order is stable.
    entries = sorted(entries, key=lambda t: t[0])

    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w") as zf:
        for arcname, src in entries:
            data = src.read_bytes()
            zf.writestr(_zipinfo(arcname), data)


def run(params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else {}
    bound = inputs.get("bound_outputs") if "bound_outputs" in inputs else params.get("bound_outputs")

    if not isinstance(bound, list) or not bound:
        return _error(outputs_dir, "missing_required_input", "Input 'bound_outputs' is required and must be a non-empty list")

    staging = outputs_dir / "_staging"
    if staging.exists():
        # Clean staging deterministically.
        for root, dirs, files in os.walk(staging, topdown=False):
            for fn in files:
                Path(root, fn).unlink(missing_ok=True)
            for dn in dirs:
                Path(root, dn).rmdir()
        staging.rmdir()
    staging.mkdir(parents=True, exist_ok=True)

    files_meta: List[Dict[str, Any]] = []
    missing_outputs: List[Dict[str, Any]] = []

    for i, item in enumerate(bound):
        if not isinstance(item, dict):
            return _error(outputs_dir, "bad_input_format", f"bound_outputs[{i}] must be an object")

        # Allow either raw OutputRecord dict or a dataclass dict.
        rec = dict(item)

        uri = str(rec.get("uri") or "").strip()
        if not uri and isinstance(rec.get("record"), dict):
            uri = str((rec.get("record") or {}).get("uri") or "").strip()
        if not uri:
            return _error(outputs_dir, "bad_input_format", f"bound_outputs[{i}] missing uri")

        try:
            src_path = _uri_to_path(uri)
        except Exception as e:
            return _error(outputs_dir, "bad_input_format", f"bound_outputs[{i}] invalid uri: {e}")

        if not src_path.exists() or not src_path.is_file():
            missing_outputs.append({
                "step_id": str(rec.get("step_id") or rec.get("from_step") or "").strip(),
                "output_id": str(rec.get("output_id") or rec.get("from_output_id") or "").strip(),
                "index": i,
            })
            continue

        as_path = rec.get("as_path")
        if as_path is None:
            as_path = rec.get("as")
        dest = _default_dest(rec, src_path) if not as_path else str(as_path)

        try:
            dest_path = _safe_dest_path(dest)
        except Exception as e:
            return _error(outputs_dir, "bad_input_format", f"bound_outputs[{i}] invalid as_path: {e}")

        dest_abs = staging / dest_path
        dest_abs.parent.mkdir(parents=True, exist_ok=True)
        dest_abs.write_bytes(src_path.read_bytes())

        sha = sha256_file(dest_abs)
        bs = int(dest_abs.stat().st_size)

        files_meta.append(
            {
                "dest_path": dest_path,
                "bytes": bs,
                "sha256": sha,
                "content_type": str(rec.get("content_type") or ""),
                "source_step_id": str(rec.get("step_id") or ""),
                "source_module_id": str(rec.get("module_id") or ""),
                "source_output_id": str(rec.get("output_id") or ""),
                "source_path": str(rec.get("path") or ""),
                "source_uri": uri,
            }
        )

    if missing_outputs:
        # Runtime validation: bound_outputs referenced outputs that cannot be packaged.
        # Provide a structured payload listing missing {step_id, output_id} pairs.
        missing_pairs = [
            {"step_id": str(m.get("step_id") or ""), "output_id": str(m.get("output_id") or "")}
            for m in missing_outputs
        ]
        return _fail(
            outputs_dir,
            "package_failed",
            {
                "message": "One or more bound outputs were missing at runtime",
                "missing_outputs": missing_pairs,
            },
        )

    # Deterministic ordering.
    files_meta = sorted(files_meta, key=lambda r: (str(r.get("dest_path") or ""), str(r.get("sha256") or "")))

    manifest = {
        "schema_version": 1,
        "module_id": MODULE_ID,
        "files": files_meta,
    }

    manifest_json_path = outputs_dir / "manifest.json"
    manifest_csv_path = outputs_dir / "manifest.csv"
    package_zip_path = outputs_dir / "package.zip"

    _write_manifest_json(manifest_json_path, manifest)
    _write_manifest_csv(manifest_csv_path, files_meta)
    _zip_deterministic(package_zip_path, manifest_json_path, manifest_csv_path, staging, files_meta)

    return {
        "status": "COMPLETED",
        "files": ["package.zip", "manifest.json", "manifest.csv"],
        "metadata": {
            "module_id": MODULE_ID,
            "file_count": len(files_meta),
        },
    }
