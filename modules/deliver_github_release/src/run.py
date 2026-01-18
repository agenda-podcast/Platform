from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

MODULE_ID = "deliver_github_release"
MAX_PACKAGE_BYTES = 262144000


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _as_local_path(inp: Any) -> Tuple[Path, Dict[str, Any]]:
    meta: Dict[str, Any] = {}
    if inp is None:
        return Path(""), meta
    if isinstance(inp, str):
        return Path(inp), meta
    if isinstance(inp, dict):
        meta = dict(inp)
        uri = str(inp.get("uri") or "").strip()
        if uri.startswith("file://"):
            return Path(uri[len("file://"):]), meta
        p = str(inp.get("path") or "").strip()
        if p:
            if not Path(p).is_absolute() and uri.startswith("file://"):
                return Path(uri[len("file://"):]), meta
            return Path(p), meta
        if uri:
            return Path(uri), meta
    return Path(str(inp)), meta


def _fail(outputs_dir: Path, *, reason_slug: str, message: str, delivery_log: Dict[str, Any]) -> Dict[str, Any]:
    ended_at = _utcnow_iso()
    delivery_log["ended_at"] = ended_at
    delivery_log["status"] = "FAILED"
    delivery_log["reason_slug"] = reason_slug
    delivery_log["message"] = message
    _write_json(outputs_dir / "delivery_log.json", delivery_log)

    receipt = {
        "schema_version": 1,
        "module_id": MODULE_ID,
        "tenant_id": delivery_log.get("tenant_id", ""),
        "work_order_id": delivery_log.get("work_order_id", ""),
        "step_id": delivery_log.get("step_id", ""),
        "module_run_id": delivery_log.get("module_run_id", ""),
        "status": "FAILED",
        "reason_slug": reason_slug,
        "message": message,
        "started_at": delivery_log.get("started_at", ""),
        "ended_at": ended_at,
    }
    _write_json(outputs_dir / "delivery_receipt.json", receipt)

    report = {
        "type": "DeliveryError",
        "module_id": MODULE_ID,
        "reason_slug": reason_slug,
        "message": message,
        "tenant_id": delivery_log.get("tenant_id", ""),
        "work_order_id": delivery_log.get("work_order_id", ""),
        "step_id": delivery_log.get("step_id", ""),
        "module_run_id": delivery_log.get("module_run_id", ""),
    }
    _write_json(outputs_dir / "report.json", report)

    return {
        "status": "FAILED",
        "reason_slug": reason_slug,
        "message": message,
        "refund_eligible": True,
    }


def _request_json(method: str, url: str, token: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _upload_asset(upload_url_template: str, token: str, *, name: str, content_type: str, file_path: Path) -> Dict[str, Any]:
    # upload_url comes like: https://uploads.github.com/repos/{owner}/{repo}/releases/{id}/assets{?name,label}
    base = re.sub(r"\{\?.*\}$", "", upload_url_template)
    url = f"{base}?name={urllib.parse.quote(name)}"

    req = urllib.request.Request(url, data=file_path.read_bytes(), method="POST")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _deterministic_tag(tenant_id: str, work_order_id: str, module_run_id: str) -> str:
    # Keep short, deterministic, and safe for Git tags.
    raw = f"verify-{tenant_id}-{work_order_id}-{module_run_id}".lower()
    raw = re.sub(r"[^a-z0-9._-]+", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-")
    return raw[:128] or "verify"


def run(*, params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    outputs_dir.mkdir(parents=True, exist_ok=True)

    tenant_id = str(params.get("tenant_id") or "")
    work_order_id = str(params.get("work_order_id") or "")
    step_id = str(params.get("step_id") or "")
    module_run_id = str(params.get("module_run_id") or "")

    inputs = params.get("inputs") or {}
    pkg_path, pkg_meta = _as_local_path(inputs.get("package_zip"))
    manifest_path, _ = _as_local_path(inputs.get("manifest_json"))

    delivery_log: Dict[str, Any] = {
        "schema_version": 1,
        "module_id": MODULE_ID,
        "tenant_id": tenant_id,
        "work_order_id": work_order_id,
        "step_id": step_id,
        "module_run_id": module_run_id,
        "started_at": _utcnow_iso(),
        "ended_at": "",
        "status": "RUNNING",
        "reason_slug": "",
        "message": "",
        "stage": "init",
        "inputs_present": {
            "package_zip": bool(str(pkg_path).strip()),
            "manifest_json": bool(str(manifest_path).strip()),
        },
        "package_meta": pkg_meta,
    }

    token = str(os.environ.get("GITHUB_TOKEN") or "").strip()
    repo = str(os.environ.get("GITHUB_REPOSITORY") or "").strip()

    delivery_log["env"] = {
        "GITHUB_TOKEN_set": bool(token),
        "GITHUB_REPOSITORY": repo,
    }

    if not pkg_path or str(pkg_path).strip() == "":
        return _fail(outputs_dir, reason_slug="missing_input", message="package_zip input is required", delivery_log=delivery_log)
    if not pkg_path.exists():
        return _fail(outputs_dir, reason_slug="missing_input", message=f"package_zip not found: {pkg_path}", delivery_log=delivery_log)

    try:
        bytes_stat = int(pkg_path.stat().st_size)
    except Exception:
        bytes_stat = 0

    bytes_hint = int(str(pkg_meta.get("bytes") or "0").strip() or 0)
    bytes_for_gate = int(bytes_hint or bytes_stat)
    delivery_log["package_bytes"] = bytes_stat
    delivery_log["package_bytes_hint"] = bytes_hint

    if bytes_for_gate >= MAX_PACKAGE_BYTES:
        return _fail(outputs_dir, reason_slug="package_too_large", message=f"package bytes {bytes_for_gate} exceed threshold {MAX_PACKAGE_BYTES}", delivery_log=delivery_log)

    pkg_sha256 = _sha256_file(pkg_path)
    delivery_log["package_sha256"] = pkg_sha256

    release_tag = str(inputs.get("release_tag") or "auto").strip()
    if release_tag == "" or release_tag.lower() == "auto":
        release_tag = _deterministic_tag(tenant_id, work_order_id, module_run_id)

    release_name = str(inputs.get("release_name") or "").strip() or release_tag
    release_notes = str(inputs.get("release_notes") or "").rstrip()

    delivery_log["release"] = {
        "tag": release_tag,
        "name": release_name,
    }

    # Dev stub mode: no token or no repo context.
    if not token or not repo:
        delivery_log["stage"] = "dev_stub"
        ended_at = _utcnow_iso()
        delivery_log["status"] = "COMPLETED"
        delivery_log["ended_at"] = ended_at
        _write_json(outputs_dir / "delivery_log.json", delivery_log)

        outbox = outputs_dir / "outbox"
        outbox.mkdir(parents=True, exist_ok=True)
        _write_json(outbox / "release_stub.json", {
            "provider": "github_release_stub",
            "tag": release_tag,
            "name": release_name,
            "notes": release_notes,
            "package_sha256": pkg_sha256,
        })

        receipt = {
            "schema_version": 1,
            "provider": "github_release_stub",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": ended_at,
            "verification_status": "stubbed",
            "remote_path": str(outbox),
            "remote_object_id": "",
            "bytes": bytes_stat,
            "sha256": pkg_sha256,
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "reason_slug": "",
            "message": "stubbed",
            "started_at": delivery_log["started_at"],
            "ended_at": ended_at,
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        report = {
            "type": "DeliveryReport",
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "message": "stubbed",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "package_bytes": bytes_stat,
        }
        _write_json(outputs_dir / "report.json", report)

        return {
            "status": "COMPLETED",
            "report_path": "report.json",
            "output_ref": str(outputs_dir),
            "refund_eligible": False,
        }

    try:
        api_base = f"https://api.github.com/repos/{repo}"

        delivery_log["stage"] = "create_release"
        rel = _request_json(
            "POST",
            f"{api_base}/releases",
            token,
            {
                "tag_name": release_tag,
                "name": release_name,
                "body": release_notes,
                "draft": False,
                "prerelease": False,
            },
        )

        upload_url = str(rel.get("upload_url") or "")
        html_url = str(rel.get("html_url") or "")
        release_id = str(rel.get("id") or "")

        assets: Dict[str, Any] = {}

        delivery_log["stage"] = "upload_assets"
        assets["package_zip"] = _upload_asset(upload_url, token, name="package.zip", content_type="application/zip", file_path=pkg_path)

        if manifest_path and str(manifest_path).strip() and manifest_path.exists():
            assets["manifest_json"] = _upload_asset(upload_url, token, name="manifest.json", content_type="application/json", file_path=manifest_path)

        ended_at = _utcnow_iso()
        delivery_log["status"] = "COMPLETED"
        delivery_log["ended_at"] = ended_at
        delivery_log["release"]["html_url"] = html_url
        delivery_log["release"]["id"] = release_id
        delivery_log["assets"] = {
            k: {
                "id": str(v.get("id") or ""),
                "name": str(v.get("name") or ""),
                "browser_download_url": str(v.get("browser_download_url") or ""),
                "size": int(v.get("size") or 0),
            }
            for k, v in assets.items()
        }

        _write_json(outputs_dir / "delivery_log.json", delivery_log)

        receipt = {
            "schema_version": 1,
            "provider": "github_release",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": ended_at,
            "verification_status": "published",
            "remote_path": html_url,
            "remote_object_id": release_id,
            "bytes": bytes_stat,
            "sha256": pkg_sha256,
            "assets": {
                k: {
                    "name": str(v.get("name") or ""),
                    "browser_download_url": str(v.get("browser_download_url") or ""),
                    "size": int(v.get("size") or 0),
                }
                for k, v in assets.items()
            },
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "reason_slug": "",
            "message": "published",
            "started_at": delivery_log["started_at"],
            "ended_at": ended_at,
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        report = {
            "type": "DeliveryReport",
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "message": "published",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "release_url": html_url,
            "package_bytes": bytes_stat,
        }
        _write_json(outputs_dir / "report.json", report)

        return {
            "status": "COMPLETED",
            "report_path": "report.json",
            "output_ref": str(outputs_dir),
            "refund_eligible": False,
        }

    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return _fail(outputs_dir, reason_slug="delivery_failed", message=f"HTTPError {e.code}: {body}", delivery_log=delivery_log)
    except Exception as e:
        return _fail(outputs_dir, reason_slug="delivery_failed", message=f"{type(e).__name__}: {e}", delivery_log=delivery_log)
