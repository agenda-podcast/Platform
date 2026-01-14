from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from onedrive_client import (
    OneDriveClient,
    OneDriveMetadata,
    OneDrivePermanentError,
    OneDriveTransientError,
    default_client,
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _as_local_path(inp: Any) -> Tuple[Path, Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    if isinstance(inp, dict):
        meta = dict(inp)
        if meta.get("as_path"):
            return Path(str(meta["as_path"])), meta
        uri = str(meta.get("uri") or "").strip()
        if uri.startswith("file://"):
            return Path(uri.replace("file://", "", 1)), meta
        if meta.get("path"):
            p = Path(str(meta["path"]))
            if p.is_absolute():
                return p, meta
            return p, meta
        raise ValueError("Unsupported file input dict (expected path/as_path/uri)")
    if isinstance(inp, str) and inp.strip():
        return Path(inp.strip()), meta
    raise ValueError("Missing required file input")

def _int_or(v: Any, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def _sha256_file(p: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _deterministic_remote_path(
    *,
    base_path: str,
    tenant_id: str,
    work_order_id: str,
    run_id: str,
    step_id: str,
    deliverable_id: str,
) -> str:
    """Deterministic remote key spec (hard-locked).

    Spec:
      {base_path}/{tenant_id}/{work_order_id}/{run_id}/{step_id}/{deliverable_id}/package.zip
    """
    parts = [
        str(base_path or "").strip(),
        str(tenant_id or "").strip(),
        str(work_order_id or "").strip(),
        str(run_id or "").strip(),
        str(step_id or "").strip(),
        str(deliverable_id or "").strip(),
        "package.zip",
    ]
    cleaned = [p.strip("/") for p in parts if p and p.strip("/")]
    return "/" + "/".join(cleaned)


def _verify(client: OneDriveClient, remote_path: str, expected_size: int, expected_sha256: str) -> Optional[OneDriveMetadata]:
    md = client.get_metadata(remote_path=remote_path)
    if not md:
        return None
    if int(md.size) != int(expected_size):
        return None
    md_sha = str(getattr(md, "sha256", "") or "").strip()
    exp_sha = str(expected_sha256 or "").strip()
    if md_sha and exp_sha and md_sha != exp_sha:
        return None
    return md


def _verified_non_delivery(client: Optional[OneDriveClient], remote_path: str) -> bool:
    if not client:
        return False
    try:
        md = client.get_metadata(remote_path=remote_path)
        return md is None
    except Exception:
        return False


def run(*, params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else params
    tenant_id = str(params.get("tenant_id") or "").strip()
    work_order_id = str(params.get("work_order_id") or "").strip()
    step_id = str((params.get("_platform") or {}).get("step_id") or "").strip()
    module_run_id = str(params.get("module_run_id") or "").strip()
    run_id = str((params.get("_platform") or {}).get("run_id") or params.get("run_id") or module_run_id or "").strip()

    report_path = "report.json"

    client: Optional[OneDriveClient] = None
    remote_path = ""

    try:
        pkg_val = (inputs or {}).get("package_zip")
        pkg_path, pkg_meta = _as_local_path(pkg_val)
        if not pkg_path.exists():
            raise FileNotFoundError(str(pkg_path))

        actual_bytes = int(pkg_path.stat().st_size)
        bytes_hint = _int_or(pkg_meta.get("bytes"), 0)
        pkg_bytes = actual_bytes

        sha_hint = str(pkg_meta.get("sha256") or "").strip()
        pkg_sha256 = _sha256_file(pkg_path)

        base_path = str((inputs or {}).get("remote_base_path") or "/Apps/Platform").strip()

        remote_path = _deterministic_remote_path(
            base_path=base_path,
            tenant_id=tenant_id,
            work_order_id=work_order_id,
            run_id=run_id,
            step_id=step_id,
            deliverable_id="package_zip",
        )

        chunk_bytes = _int_or(os.environ.get("ONEDRIVE_CHUNK_BYTES"), 4 * 1024 * 1024)
        create_share_link = str(os.environ.get("ONEDRIVE_CREATE_SHARE_LINK") or "false").strip().lower() == "true"

        client = default_client(outputs_dir)

        verification_status = "unverified"
        share_url = ""

        try:
            md0 = client.get_metadata(remote_path=remote_path)
            md0_sha = str(getattr(md0, "sha256", "") or "").strip() if md0 else ""
            if md0 and int(getattr(md0, "size", 0) or 0) == int(pkg_bytes) and (not md0_sha or md0_sha == pkg_sha256):
                verification_status = "already_exists_verified"
                md = md0
            else:
                client.upload_resumable(local_path=pkg_path, remote_path=remote_path, chunk_bytes=chunk_bytes)
                md = _verify(client, remote_path, pkg_bytes, pkg_sha256)
                if not md:
                    raise OneDriveTransientError("post-upload verification failed")
                verification_status = "verified"
        except OneDriveTransientError as e:
            md = _verify(client, remote_path, pkg_bytes, pkg_sha256)
            if md:
                verification_status = "verified_after_transient_error"
            else:
                raise OneDriveTransientError(str(e))

        if create_share_link:
            try:
                share_url = client.create_or_get_share_link(remote_path=remote_path) or ""
            except Exception:
                share_url = ""

        receipt: Dict[str, Any] = {
            "schema_version": 1,
            "provider": "onedrive",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": _utcnow_iso(),
            "verification_status": verification_status,
            "remote_path": remote_path,
            "remote_object_id": "",
            "share_link": share_url,
            "bytes": int(pkg_bytes),
            "bytes_hint": int(bytes_hint),
            "sha256": pkg_sha256,
            "sha256_hint": sha_hint,
            "share_url": share_url,
            "package": {"path": str(pkg_path), "bytes": int(pkg_bytes), "sha256": pkg_sha256},
        }

        md_final = client.get_metadata(remote_path=remote_path)
        if md_final:
            receipt["remote_object_id"] = str(getattr(md_final, "item_id", "") or "")
            if not share_url:
                receipt["share_link"] = str(getattr(md_final, "web_url", "") or "") or ""
                receipt["share_url"] = receipt["share_link"]

        _write_json(outputs_dir / "delivery_receipt.json", receipt)
        return {
            "status": "COMPLETED",
            "outputs": {"delivery_receipt_json": {"path": "delivery_receipt.json"}},
        }

    except (OneDriveTransientError, OneDrivePermanentError, FileNotFoundError, ValueError, OSError) as e:
        err = {
            "type": e.__class__.__name__,
            "reason_slug": "delivery_failed",
            "message": str(e),
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
        }
        _write_json(outputs_dir / report_path, err)
        refund_eligible = _verified_non_delivery(client, remote_path)
        return {
            "status": "FAILED",
            "reason_slug": "delivery_failed",
            "report_path": report_path,
            "output_ref": str(outputs_dir),
            "refund_eligible": refund_eligible,
        }
