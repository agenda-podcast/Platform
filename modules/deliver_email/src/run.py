from __future__ import annotations

import json
import os
import smtplib
import hashlib
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, Tuple


MODULE_ID = "deliver_email"
MAX_PACKAGE_BYTES = 20866662  # 19.9 MiB-ish safety threshold for GitHub/email constraints


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _as_local_path(inp: Any) -> Tuple[Path, Dict[str, Any]]:
    """
    Best-effort adapter for orchestrator input binding formats.

    Accepts:
      - string path
      - dict with 'path' or 'uri'
    """
    meta: Dict[str, Any] = {}
    if inp is None:
        return Path(""), meta

    if isinstance(inp, str):
        return Path(inp), meta

    if isinstance(inp, dict):
        meta = dict(inp)
        uri = str(inp.get("uri") or "").strip()
        if uri.startswith("file://"):
            # file://<abs-path>
            return Path(uri[len("file://"):]), meta
        p = str(inp.get("path") or "").strip()
        if p:
            # If the resolved binding provided a relative path, prefer any absolute 'uri' (if present)
            # because module execution cwd is not the producing step's outputs directory.
            if not Path(p).is_absolute() and str(uri).startswith("file://"):
                return Path(uri[len("file://"):]), meta
            return Path(p), meta
        if uri:
            return Path(uri), meta

    return Path(str(inp)), meta


def _int_or_none(v: Any) -> int | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


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
        "report_path": "report.json",
        "output_ref": str(outputs_dir),
        "refund_eligible": True,
    }


def _send_via_smtp(
    *,
    host: str,
    port: int,
    use_tls: bool,
    username: str,
    password: str,
    from_email: str,
    to_email: str,
    subject: str,
    body: str,
    attachment_path: Path,
) -> str:
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Date"] = formatdate(localtime=False)
    msg["Message-ID"] = make_msgid(domain=from_email.split("@")[-1] if "@" in from_email else None)
    msg["Subject"] = subject

    msg.set_content(body or "")

    attachment_bytes = attachment_path.read_bytes()
    msg.add_attachment(
        attachment_bytes,
        maintype="application",
        subtype="zip",
        filename=attachment_path.name,
    )

    with smtplib.SMTP(host=host, port=port, timeout=30) as s:
        s.ehlo()
        if use_tls:
            s.starttls()
            s.ehlo()
        if username:
            # If username provided, password must be set; validate earlier.
            s.login(username, password)
        s.send_message(msg)

    return str(msg["Message-ID"] or "")


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
            "recipient_email": bool(str(inputs.get("recipient_email") or "").strip()),
        },
    }

    # Resolve env vars (injected by secretstore for this module run).
    from_email = str(os.environ.get("EMAIL_FROM_EMAIL") or "").strip()
    smtp_host = str(os.environ.get("EMAIL_SMTP_HOST") or "").strip()
    smtp_port = _int_or_none(os.environ.get("EMAIL_SMTP_PORT"))
    use_tls = str(os.environ.get("EMAIL_SMTP_USE_TLS") or "true").strip().lower() == "true"
    username = str(os.environ.get("EMAIL_SMTP_USERNAME") or "").strip()
    password = str(os.environ.get("EMAIL_SMTP_PASSWORD") or "").strip()

    delivery_log["config"] = {
        "from_email": from_email,
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "use_tls": use_tls,
        "auth_enabled": bool(username),
        "username_set": bool(username),
    }
    delivery_log["secrets_presence"] = {
        "EMAIL_FROM_EMAIL": bool(from_email),
        "EMAIL_SMTP_HOST": bool(smtp_host),
        "EMAIL_SMTP_PORT": bool(smtp_port),
        "EMAIL_SMTP_USERNAME": bool(username),
        "EMAIL_SMTP_PASSWORD": bool(password),
    }

    if not pkg_path or str(pkg_path).strip() == "":
        return _fail(outputs_dir, reason_slug="missing_input", message="package_zip input is required", delivery_log=delivery_log)

    if not pkg_path.exists():
        return _fail(outputs_dir, reason_slug="missing_input", message=f"package_zip not found: {pkg_path}", delivery_log=delivery_log)

    try:
        bytes_stat = pkg_path.stat().st_size
    except Exception:
        bytes_stat = 0

    delivery_log["package_path"] = str(pkg_path)
    delivery_log["package_bytes"] = int(bytes_stat)
    delivery_log["package_meta"] = pkg_meta

    bytes_hint = _int_or_none(pkg_meta.get("bytes")) or 0
    delivery_log["package_bytes_hint"] = int(bytes_hint)

    # Size gate: if an orchestrator provided a bytes hint, enforce it without reading the file.
    bytes_for_gate = int(bytes_hint or bytes_stat)
    if bytes_for_gate >= MAX_PACKAGE_BYTES:
        return _fail(
            outputs_dir,
            reason_slug="package_too_large_for_email",
            message=f"package bytes {bytes_for_gate} exceed threshold {MAX_PACKAGE_BYTES}",
            delivery_log=delivery_log,
        )

    pkg_sha256 = ""
    try:
        if bytes_stat > 0:
            pkg_sha256 = _sha256_file(pkg_path)
    except Exception:
        pkg_sha256 = ""

    # Secondary size gate on actual file size (defense-in-depth).
    if bytes_stat >= MAX_PACKAGE_BYTES:
        return _fail(
            outputs_dir,
            reason_slug="package_too_large_for_email",
            message=f"package bytes {bytes_stat} exceed threshold {MAX_PACKAGE_BYTES}",
            delivery_log=delivery_log,
        )

    recipient = str(inputs.get("recipient_email") or "").strip()
    if not recipient:
        recipient = str(os.environ.get("DELIVER_EMAIL_DEFAULT_RECIPIENT") or "").strip()
    if not recipient:
        return _fail(outputs_dir, reason_slug="missing_input", message="recipient_email is required", delivery_log=delivery_log)

    if not from_email:
        # In stub mode, from_email is optional. If not provided, use a deterministic placeholder.
        from_email = "no-reply@example.invalid"

    # Dev outbox stub: if SMTP host/port are not configured, write an outbox artifact and succeed.
    if not smtp_host or not smtp_port:
        delivery_log["stage"] = "stub_outbox"
        outbox_dir = outputs_dir / "outbox"
        outbox_dir.mkdir(parents=True, exist_ok=True)
        stub_path = outbox_dir / f"{module_run_id or 'delivery'}.eml"
        stub_path.write_text(
            "\n".join(
                [
                    f"From: {from_email}",
                    f"To: {recipient}",
                    f"Subject: Platform delivery: {work_order_id} [{step_id}]",
                    "",
                    "SMTP not configured. This is a dev stub delivery.",
                    f"Attachment: {pkg_path.name}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        delivered_at = _utcnow_iso()
        message_id = f"outbox_stub:{module_run_id or 'delivery'}"
        receipt = {
            "schema_version": 1,
            "provider": "outbox_stub",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": delivered_at,
            "verification_status": "written",
            "remote_path": str(stub_path),
            "remote_object_id": message_id,
            "bytes": int(bytes_stat),
            "sha256": pkg_sha256,

            # Backward-compatible fields used by earlier modules.
            "status": "COMPLETED",
            "message": "stub_outbox_written",
            "recipient_email": recipient,
            "message_id": message_id,
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        delivery_log["status"] = "COMPLETED"
        delivery_log["ended_at"] = delivered_at
        _write_json(outputs_dir / "delivery_log.json", delivery_log)

        report = {
            "type": "DeliveryReport",
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "mode": "stub_outbox",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "recipient_email": recipient,
            "package_bytes": int(bytes_stat),
        }
        _write_json(outputs_dir / "report.json", report)

        return {
            "status": "COMPLETED",
            "report_path": "report.json",
            "output_ref": str(outputs_dir),
            "refund_eligible": False,
        }

    if username and not password:
        return _fail(outputs_dir, reason_slug="secrets_missing", message="EMAIL_SMTP_PASSWORD is required when EMAIL_SMTP_USERNAME is set", delivery_log=delivery_log)

    subject = str(inputs.get("subject") or f"Platform delivery: {work_order_id} [{step_id}]").strip()
    body = str(inputs.get("body") or "").strip()

    delivery_log["recipient_email"] = recipient
    delivery_log["subject"] = subject
    delivery_log["stage"] = "smtp_send"

    try:
        msg_id = _send_via_smtp(
            host=smtp_host,
            port=int(smtp_port),
            use_tls=use_tls,
            username=username,
            password=password,
            from_email=from_email,
            to_email=recipient,
            subject=subject,
            body=body,
            attachment_path=pkg_path,
        )
        delivery_log["message_id"] = msg_id
        delivery_log["stage"] = "done"
        delivery_log["status"] = "COMPLETED"
        delivery_log["ended_at"] = _utcnow_iso()

        _write_json(outputs_dir / "delivery_log.json", delivery_log)

        delivered_at = delivery_log["ended_at"]
        receipt = {
            "schema_version": 1,
            "provider": "email",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": delivered_at,
            "verification_status": "sent",
            "remote_path": recipient,
            "remote_object_id": msg_id,
            "bytes": int(bytes_stat),
            "sha256": pkg_sha256,

            # Backward-compatible fields.
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "reason_slug": "",
            "message": "sent",
            "started_at": delivery_log["started_at"],
            "ended_at": delivery_log["ended_at"],
            "message_id": msg_id,
            "recipient_email": recipient,
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        # report.json is still useful on success to show what happened.
        report = {
            "type": "DeliveryReport",
            "module_id": MODULE_ID,
            "status": "COMPLETED",
            "message": "sent",
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "message_id": msg_id,
            "package_bytes": int(bytes_stat),
        }
        _write_json(outputs_dir / "report.json", report)

        return {
            "status": "COMPLETED",
            "report_path": "report.json",
            "output_ref": str(outputs_dir),
            "refund_eligible": False,
        }
    except Exception as e:
        return _fail(outputs_dir, reason_slug="delivery_failed", message=f"{type(e).__name__}: {e}", delivery_log=delivery_log)
