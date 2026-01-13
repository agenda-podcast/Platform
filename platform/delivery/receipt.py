from __future__ import annotations

from typing import Any, Dict


class DeliveryReceiptValidationError(ValueError):
    pass


def validate_delivery_receipt(receipt: Dict[str, Any]) -> None:
    """Validate delivery receipt payloads written by delivery modules.

    Hard-lock schema requirements used by unit tests.

    Required keys:
      - provider
      - delivered_at
      - verification_status
      - bytes
      - sha256
      - remote_path OR remote_object_id
    """
    if not isinstance(receipt, dict):
        raise DeliveryReceiptValidationError("receipt must be a dict")

    provider = str(receipt.get("provider") or "").strip()
    if not provider:
        raise DeliveryReceiptValidationError("missing provider")

    delivered_at = str(receipt.get("delivered_at") or "").strip()
    if not delivered_at:
        raise DeliveryReceiptValidationError("missing delivered_at")

    verification_status = str(receipt.get("verification_status") or "").strip()
    if not verification_status:
        raise DeliveryReceiptValidationError("missing verification_status")

    remote_path = str(receipt.get("remote_path") or "").strip()
    remote_object_id = str(receipt.get("remote_object_id") or "").strip()
    if not remote_path and not remote_object_id:
        raise DeliveryReceiptValidationError("missing remote_path/remote_object_id")

    try:
        b = int(receipt.get("bytes") or 0)
    except Exception:
        b = 0
    if b <= 0:
        raise DeliveryReceiptValidationError("invalid bytes")

    sha256 = str(receipt.get("sha256") or "").strip()
    if len(sha256) != 64:
        raise DeliveryReceiptValidationError("invalid sha256")

    # Provider-specific optional keys are allowed.
