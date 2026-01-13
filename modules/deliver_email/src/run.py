from __future__ import annotations

import json
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _as_local_path(inp: Any) -> Tuple[Path, Dict[str, Any]]:
    """Resolve an input that may be a string path or an OutputRecord-like dict."""
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


def _int_or_none(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _sha256_file(p: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _send_via_smtp(*, host: str, port: int, use_tls: bool, username: str, password: str, from_email: str, to_email: str, subject: str, body: str, attachment_path: Path) -> str:
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=False)
    message_id = make_msgid(domain=None)
    msg["Message-ID"] = message_id
    msg.set_content(body)

    data = attachment_path.read_bytes()
    msg.add_attachment(data, maintype="application", subtype="zip", filename=attachment_path.name)

    if use_tls:
        with smtplib.SMTP(host=host, port=port, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            if username:
                s.login(username, password)
            s.send_message(msg)
            return message_id

    with smtplib.SMTP(host=host, port=port, timeout=30) as s:
        if username:
            s.login(username, password)
        s.send_message(msg)
        return message_id


def _send_to_dev_outbox(*, outputs_dir: Path, from_email: str, to_email: str, subject: str, body: str, attachment_path: Path) -> Tuple[str, str]:
    outbox = outputs_dir / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    message_id = make_msgid(domain="dev.outbox")
    eml_path = outbox / "message.eml"

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=False)
    msg["Message-ID"] = message_id
    msg.set_content(body)

    data = attachment_path.read_bytes()
    msg.add_attachment(data, maintype="application", subtype="zip", filename=attachment_path.name)

    eml_path.write_bytes(msg.as_bytes())
    return message_id, str(eml_path)


MAX_PACKAGE_BYTES = 20866662


def run(*, params: Dict[str, Any], outputs_dir: Path) -> Dict[str, Any]:
    """Deliver the provided package.zip via email.

    Policy:
      - If package bytes >= MAX_PACKAGE_BYTES, fail with reason_slug=package_too_large_for_email.
      - No link fallback.
    """
    outputs_dir.mkdir(parents=True, exist_ok=True)

    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else params
    tenant_id = str(params.get("tenant_id") or "").strip()
    work_order_id = str(params.get("work_order_id") or "").strip()
    step_id = str((params.get("_platform") or {}).get("step_id") or "").strip()
    module_run_id = str(params.get("module_run_id") or "").strip()

    report_path = "report.json"
    delivery_attempted = False

    try:
        pkg_val = (inputs or {}).get("package_zip")
        pkg_path, pkg_meta = _as_local_path(pkg_val)
        if not pkg_path.exists():
            raise FileNotFoundError(str(pkg_path))

        bytes_hint = _int_or_none(pkg_meta.get("bytes"))
        pkg_bytes = int(bytes_hint) if bytes_hint is not None and int(bytes_hint) > 0 else int(pkg_path.stat().st_size)

        # Enforce size cap before hashing or reading attachment into memory.
        if pkg_bytes >= MAX_PACKAGE_BYTES:
            err = {
                "type": "DeliveryError",
                "reason_slug": "package_too_large_for_email",
                "message": f"package bytes {pkg_bytes} exceed threshold {MAX_PACKAGE_BYTES}",
                "tenant_id": tenant_id,
                "work_order_id": work_order_id,
                "step_id": step_id,
                "module_run_id": module_run_id,
            }
            _write_json(outputs_dir / report_path, err)
            return {"status": "FAILED", "reason_slug": "package_too_large_for_email", "report_path": report_path, "output_ref": str(outputs_dir), "refund_eligible": True}

        sha_hint = str(pkg_meta.get("sha256") or "").strip()
        pkg_sha256 = sha_hint if sha_hint else _sha256_file(pkg_path)

        recipient = str((inputs or {}).get("recipient_email") or "").strip()
        if not recipient:
            recipient = str(os.environ.get("DELIVER_EMAIL_DEFAULT_RECIPIENT") or "").strip()
        if not recipient:
            raise ValueError("recipient_email is required (or set DELIVER_EMAIL_DEFAULT_RECIPIENT)")

        from_email = str(os.environ.get("EMAIL_FROM_EMAIL") or "noreply@example.com").strip() or "noreply@example.com"
        subject = f"Platform delivery: {tenant_id}/{work_order_id}"
        body = "Attached: package.zip\n"

        smtp_host = str(os.environ.get("EMAIL_SMTP_HOST") or "").strip()
        smtp_port = _int_or_none(os.environ.get("EMAIL_SMTP_PORT"))
        use_tls = str(os.environ.get("EMAIL_SMTP_USE_TLS") or "true").strip().lower() == "true"
        username = str(os.environ.get("EMAIL_SMTP_USERNAME") or "").strip()
        password = str(os.environ.get("EMAIL_SMTP_PASSWORD") or "").strip()

        provider = "outbox_stub"
        remote_object_id = ""
        remote_path = ""
        verification_status = "written"

        if smtp_host and smtp_port:
            provider = "smtp"
            verification_status = "sent"
            delivery_attempted = True
            remote_object_id = _send_via_smtp(host=smtp_host, port=int(smtp_port), use_tls=use_tls, username=username, password=password, from_email=from_email, to_email=recipient, subject=subject, body=body, attachment_path=pkg_path)
        else:
            delivery_attempted = True
            remote_object_id, remote_path = _send_to_dev_outbox(outputs_dir=outputs_dir, from_email=from_email, to_email=recipient, subject=subject, body=body, attachment_path=pkg_path)

        receipt = {
            "schema_version": 1,
            "provider": provider,
            "tenant_id": tenant_id,
            "work_order_id": work_order_id,
            "step_id": step_id,
            "module_run_id": module_run_id,
            "delivered_at": _utcnow_iso(),
            "verification_status": verification_status,
            "remote_path": remote_path,
            "remote_object_id": remote_object_id,
            "share_link": "",
            "bytes": int(pkg_bytes),
            "sha256": pkg_sha256,
            # Backward-compatible, provider-specific fields:
            "recipient_email": recipient,
            "from_email": from_email,
            "subject": subject,
            "message_id": remote_object_id,
            "package": {"path": str(pkg_path), "bytes": int(pkg_bytes), "sha256": pkg_sha256},
        }
        _write_json(outputs_dir / "delivery_receipt.json", receipt)

        return {"status": "COMPLETED", "reason_slug": "", "report_path": "", "output_ref": str(outputs_dir), "refund_eligible": False}

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
        return {"status": "FAILED", "reason_slug": "delivery_failed", "report_path": report_path, "output_ref": str(outputs_dir), "refund_eligible": False}
