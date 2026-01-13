from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from ..contracts import TenantCredentialsStore
from ..errors import ValidationError
from ...secretstore.loader import env_for_integration, load_secretstore
from ...utils.csvio import read_csv, write_csv
from ...utils.time import utcnow_iso


TENANT_INTEGRATIONS_HEADERS = [
    "provider",
    "status",
    "created_at",
    "updated_at",
    "token_updated_at",
    "integration_json",
]


def _load_passphrase(repo_root: Path) -> str:
    """Resolve the encryption passphrase.

    Precedence:
      1) TOKEN_ENCRYPTION_KEY
      2) SECRETSTORE_PASSPHRASE
      3) secretstore integration oauth_global (TOKEN_ENCRYPTION_KEY or SECRETSTORE_PASSPHRASE)

    The returned value is used for symmetric GPG encryption.
    """

    direct = str(os.environ.get("TOKEN_ENCRYPTION_KEY", "") or "").strip()
    if direct:
        return direct

    legacy = str(os.environ.get("SECRETSTORE_PASSPHRASE", "") or "").strip()
    if legacy:
        return legacy

    store = load_secretstore(repo_root)
    integ = env_for_integration(store, "oauth_global")
    v1 = str(integ.get("TOKEN_ENCRYPTION_KEY", "") or "").strip()
    if v1:
        return v1
    v2 = str(integ.get("SECRETSTORE_PASSPHRASE", "") or "").strip()
    if v2:
        return v2
    return ""


def _decrypt_gpg_json(gpg_path: Path, passphrase: str) -> Dict[str, Any]:
    if not gpg_path.exists():
        return {}
    proc = subprocess.run(
        [
            "gpg",
            "--batch",
            "--yes",
            "--pinentry-mode",
            "loopback",
            "--passphrase-fd",
            "0",
            "-d",
            str(gpg_path),
        ],
        input=passphrase,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to decrypt tenant tokens: {proc.stderr.strip()}")

    out = (proc.stdout or "").strip()
    if not out:
        return {}
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        raise RuntimeError(f"Decrypted tenant tokens are not valid JSON: {e}")


def _encrypt_gpg_json(gpg_path: Path, *, passphrase: str, payload: Dict[str, Any]) -> None:
    gpg_path.parent.mkdir(parents=True, exist_ok=True)
    plaintext = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tf:
        tmp_path = Path(tf.name)
        tf.write(plaintext)
        tf.flush()

    try:
        proc = subprocess.run(
            [
                "gpg",
                "--batch",
                "--yes",
                "--pinentry-mode",
                "loopback",
                "--passphrase-fd",
                "0",
                "--symmetric",
                "--cipher-algo",
                "AES256",
                "-o",
                str(gpg_path),
                str(tmp_path),
            ],
            input=passphrase,
            text=True,
            capture_output=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to encrypt tenant tokens: {proc.stderr.strip()}")
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


class TenantCredentialsStoreCsv(TenantCredentialsStore):
    """Dev-mode tenant credentials store.

    Storage layout per tenant:
      tenants/<tenant_id>/integrations/tenant_integrations.csv
      tenants/<tenant_id>/integrations/tokens.gpg

    The CSV stores non-sensitive integration metadata.
    The encrypted GPG file stores provider tokens (refresh tokens, access tokens).
    """

    def __init__(self, *, repo_root: Path, tenants_root: Path) -> None:
        self.repo_root = repo_root
        self.tenants_root = tenants_root

    def _tenant_dir(self, tenant_id: str) -> Path:
        tid = str(tenant_id or "").strip()
        if not tid:
            raise ValidationError("tenant_id is required")
        return self.tenants_root / tid / "integrations"

    def _integrations_csv_path(self, tenant_id: str) -> Path:
        return self._tenant_dir(tenant_id) / "tenant_integrations.csv"

    def _tokens_path(self, tenant_id: str) -> Path:
        return self._tenant_dir(tenant_id) / "tokens.gpg"

    def _load_tokens(self, tenant_id: str, *, passphrase: str) -> Dict[str, Any]:
        p = self._tokens_path(tenant_id)
        raw = _decrypt_gpg_json(p, passphrase) if p.exists() else {}
        providers = raw.get("providers") if isinstance(raw, dict) else None
        if not isinstance(providers, dict):
            providers = {}
        return {"version": int(raw.get("version", 1)) if isinstance(raw, dict) else 1, "providers": providers}

    def _save_tokens(self, tenant_id: str, *, passphrase: str, raw: Dict[str, Any]) -> None:
        _encrypt_gpg_json(self._tokens_path(tenant_id), passphrase=passphrase, payload=raw)

    def get_integration(self, tenant_id: str, provider: str) -> Optional[Dict[str, Any]]:
        prov = str(provider or "").strip().lower()
        if not prov:
            raise ValidationError("provider is required")

        csv_path = self._integrations_csv_path(tenant_id)
        rows = read_csv(csv_path)
        row = None
        for r in rows:
            if str(r.get("provider", "")).strip().lower() == prov:
                row = r
                break
        if row is None:
            return None

        integration_json = str(row.get("integration_json", "") or "").strip()
        payload: Dict[str, Any] = {}
        if integration_json:
            try:
                j = json.loads(integration_json)
                payload = j if isinstance(j, dict) else {}
            except Exception:
                payload = {}

        payload["provider"] = prov
        payload["status"] = str(row.get("status", "") or "").strip() or "active"
        payload["created_at"] = str(row.get("created_at", "") or "").strip()
        payload["updated_at"] = str(row.get("updated_at", "") or "").strip()
        payload["token_updated_at"] = str(row.get("token_updated_at", "") or "").strip()

        passphrase = _load_passphrase(self.repo_root)
        if passphrase:
            raw = self._load_tokens(tenant_id, passphrase=passphrase)
            tok = (raw.get("providers") or {}).get(prov)
            if tok is not None:
                payload["token"] = tok

        return payload

    def upsert_integration(self, *, tenant_id: str, provider: str, integration: Dict[str, Any]) -> None:
        prov = str(provider or "").strip().lower()
        if not prov:
            raise ValidationError("provider is required")

        now = utcnow_iso()
        csv_path = self._integrations_csv_path(tenant_id)
        existing_rows = read_csv(csv_path)

        created_at = now
        token_updated_at = ""
        new_rows = []
        found = False
        for r in existing_rows:
            if str(r.get("provider", "")).strip().lower() == prov:
                found = True
                created_at = str(r.get("created_at", "") or "").strip() or created_at
                token_updated_at = str(r.get("token_updated_at", "") or "").strip()
                continue
            new_rows.append(r)

        token = integration.get("token") if isinstance(integration, dict) else None
        status = str((integration.get("status") if isinstance(integration, dict) else "") or "").strip() or "active"

        # Store non-sensitive integration metadata in the CSV.
        integration_copy: Dict[str, Any] = dict(integration or {})
        if "token" in integration_copy:
            integration_copy.pop("token", None)

        if token is not None:
            passphrase = _load_passphrase(self.repo_root)
            if not passphrase:
                raise ValidationError(
                    "token encryption key is required: set TOKEN_ENCRYPTION_KEY or SECRETSTORE_PASSPHRASE (or oauth_global integration)."
                )
            raw = self._load_tokens(tenant_id, passphrase=passphrase)
            providers = raw.get("providers")
            if not isinstance(providers, dict):
                providers = {}
            providers[prov] = token
            raw["providers"] = providers
            raw.setdefault("version", 1)
            self._save_tokens(tenant_id, passphrase=passphrase, raw=raw)
            token_updated_at = now

        row_out = {
            "provider": prov,
            "status": status,
            "created_at": created_at,
            "updated_at": now,
            "token_updated_at": token_updated_at,
            "integration_json": json.dumps(integration_copy, ensure_ascii=False, separators=(",", ":")),
        }
        new_rows.append(row_out)

        # Stable ordering for determinism.
        new_rows = sorted(new_rows, key=lambda x: str(x.get("provider", "")))
        write_csv(csv_path, new_rows, TENANT_INTEGRATIONS_HEADERS)

    def describe(self) -> Dict[str, Any]:
        return {
            "class": self.__class__.__name__,
            "tenants_root": str(self.tenants_root),
            "storage": "per-tenant CSV + tokens.gpg",
        }
