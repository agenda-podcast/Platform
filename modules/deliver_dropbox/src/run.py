from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from dropbox_client import (
    DropboxPermanentError,
    DropboxTransientError,
    DropboxClient,
    DropboxMetadata,
    default_client,
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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
        s = str(v).strip()
        if not s:
            return default
        iv = int(s)
        if iv <= 0:
            return default
        return iv
    except Exception:
        return default


def _sha256_file(p: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _deterministic_remote_path(
    *,
    tenant_id: str,
    work_order_id: str,
    run_id: str,
    step_id: str,
    deliverable_id: str,
) -> str:
    """Deterministic remote key spec (hard-locked).

    Spec:
      /{tenant_id}/{work_order_id}/{run_id}/{step_id}/{deliverable_id}/package.zip
    """
    parts = [
        str(tenant_id or "").strip(),
        str(work_order_id or "").strip(),
        str(run_id or "").strip(),
        str(step_id or "").strip(),
        str(deliverable_id or "").strip(),
        "package.zip",
    ]
    cleaned = [p.strip("/") for p in parts if p and p.strip("/")]
    return "/" + "/".join(cleaned)


def _verify(client: DropboxClient, remote_path: str, expected_size: int, expected_sha256: str) -> Optional[DropboxMetadata]:
    md = client.get_metadata(remote_path=remote_path)
    if not md:
        return None
    if int(md.size) != int(expected_size):
        return None
    # If the provider metadata includes sha256, enforce it. (Dev stub provides sha256.)
    md_sha = str(getattr(md, "sha256", "") or "").strip()
    exp_sha = str(expected_sha256 or "").strip()
    if md_sha and exp_sha and md_sha != exp_sha:
        return None
    return md


def _verified_non_delivery(client: Optional[DropboxClient], remote_path: str) -> bool:
    """Return True only when we can verify the object is not present remotely.

    For Dropbox, a successful metadata lookup returning None is treated as verified non-delivery.
    Any exception or inability to check is treated as unknown and therefore not refundable.
    """
    if client is None:
        return False
    rp = str(remote_path or "").strip()
    if not rp:
        return False
    try:
        md = client.get_metadata(remote_path=rp)
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

    client: Optional[DropboxClient] = None
    remote_path = ""

    try:
        pkg_val = (inputs or {}).get("package_zip")
        pkg_path, pkg_meta = _as_local_path(pkg_val)
        if not pkg_path.exists():
            raise FileNotFoundError(str(pkg_path))

        actual_bytes = int(pkg_path.stat().st_size)
        bytes_hint = _int_or(pkg_meta.get("bytes"), 0)
        # Always trust the local file size to avoid stale/corrupt hints causing verification false-negatives.
        pkg_bytes = actual_bytes

        # Always compute sha256 from the local file.
        # In orchestrator context, the bound OutputRecord may carry a sha256 hint. If that hint
        # is stale or malformed, trusting it can cause false negatives during post-upload verification.
        sha_hint = str(pkg_meta.get("sha256") or "").strip()
        pkg_sha256 = _sha256_file(pkg_path)

        remote_path = _deterministic_remote_path(
            tenant_id=tenant_id,
            work_order_id=work_order_id,
            run_id=run_id,
            step_id=step_id,
            deliverable_id="package_zip",
        )

        chunk_bytes = _int_or(os.environ.get("DROPBOX_CHUNK_BYTES"), 4 * 1024 * 1024)
        create_share_link = str(os.environ.get("DROPBOX_CREATE_SHARE_LINK") or "false").strip().lower() == "true"

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
                    raise DropboxTransientError("post-upload verification failed")
                verification_status = "verified"
        except DropboxTransientError as e:
            # Verify loop for transient error: confirm whether the remote already exists.
            md = _verify(client, remote_path, pkg_bytes, pkg_sha256)
            if md:
                verification_status = "verified_after_transient_error"
            else:
                raise DropboxTransientError(str(e))

        if create_share_link:
            try:
                share_url = client.create_or_get_share_link(remote_path=remote_path) or ""
            except Exception:
                share_url = ""

        receipt = {
            "schema_version": 1,
            "provider": "dropbox",
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

            # Backward-compatible fields:
            "share_url": share_url,
            "package": {"path": str(pkg_path), "bytes": int(pkg_bytes), "sha256": pkg_sha256},
        }
        md = client.get_metadata(remote_path=remote_path)
        if md:
            receipt["remote"] = {"path_display": md.path_display, "size": int(md.size), "content_hash": md.content_hash}

        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        return {
            "status": "COMPLETED",
            "reason_slug": "",
            "report_path": "",
            "output_ref": str(outputs_dir),
            "refund_eligible": False,
        }

    except DropboxPermanentError as e:
        err = {
            "type": "DropboxError",
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
    except DropboxTransientError as e:
        err = {
            "type": "DropboxTransientError",
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
    except Exception as e:
        err = {
            "type": "DeliveryError",
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
