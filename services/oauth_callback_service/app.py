from __future__ import annotations

import base64
import hmac
import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse


app = FastAPI(title="Platform OAuth Callback Service", version="0.1.0")


DROPBOX_AUTH_URL = "https://www.dropbox.com/oauth2/authorize"
DROPBOX_TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"


def _repo_root() -> Path:
    return Path(str(os.environ.get("PLATFORM_REPO_ROOT", ".") or ".")).expanduser().resolve()


def _load_integration_env(integration_id: str) -> Dict[str, str]:
    try:
        from platform.secretstore.loader import env_for_integration, load_secretstore

        store = load_secretstore(_repo_root())
        return env_for_integration(store, integration_id)
    except Exception:
        return {}


def _get_setting(key: str, *, integration_id: str) -> str:
    direct = str(os.environ.get(key, "") or "").strip()
    if direct:
        return direct
    integ = _load_integration_env(integration_id)
    return str(integ.get(key, "") or "").strip()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _state_sign(payload: Dict[str, Any], signing_key: str) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    msg = _b64url(raw)
    sig = hmac.new(signing_key.encode("utf-8"), msg.encode("ascii"), sha256).digest()
    return msg + "." + _b64url(sig)


def _state_verify(state: str, signing_key: str) -> Dict[str, Any]:
    parts = str(state or "").split(".")
    if len(parts) != 2:
        raise HTTPException(status_code=400, detail="invalid_state_format")
    msg, sig = parts
    expected = hmac.new(signing_key.encode("utf-8"), msg.encode("ascii"), sha256).digest()
    got = _b64url_decode(sig)
    if not hmac.compare_digest(expected, got):
        raise HTTPException(status_code=400, detail="invalid_state_signature")
    try:
        payload = json.loads(_b64url_decode(msg).decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_state_payload")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid_state_payload")
    return payload


def _redirect_uri(request: Request, provider: str) -> str:
    base = str(os.environ.get("OAUTH_REDIRECT_BASE", "") or "").strip()
    if base:
        return base.rstrip("/") + f"/callback/{provider}"
    return str(request.url_for("oauth_callback", provider=provider))


@app.get("/start/{provider}")
async def start(provider: str, request: Request, tenant_id: str, scopes: Optional[str] = None) -> RedirectResponse:
    prov = str(provider or "").strip().lower()
    if prov != "dropbox":
        raise HTTPException(status_code=400, detail="unsupported_provider")

    signing_key = _get_setting("OAUTH_STATE_SIGNING_KEY", integration_id="oauth_global")
    if not signing_key:
        raise HTTPException(status_code=500, detail="missing_OAUTH_STATE_SIGNING_KEY")

    client_id = _get_setting("DROPBOX_APP_KEY", integration_id="oauth_dropbox")
    if not client_id:
        raise HTTPException(status_code=500, detail="missing_DROPBOX_APP_KEY")

    state = _state_sign({"tenant_id": tenant_id, "provider": prov}, signing_key)

    redirect_uri = _redirect_uri(request, prov)
    scope_str = (scopes or _get_setting("DROPBOX_SCOPES", integration_id="oauth_dropbox")).strip()

    params = {
        "client_id": client_id,
        "response_type": "code",
        "token_access_type": "offline",
        "redirect_uri": redirect_uri,
        "state": state,
    }
    if scope_str:
        params["scope"] = scope_str

    url = DROPBOX_AUTH_URL + "?" + urlencode(params)
    return RedirectResponse(url=url, status_code=302)


@app.get("/callback/{provider}", name="oauth_callback")
async def callback(provider: str, request: Request, code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None) -> JSONResponse:
    prov = str(provider or "").strip().lower()
    if prov != "dropbox":
        raise HTTPException(status_code=400, detail="unsupported_provider")

    if error:
        raise HTTPException(status_code=400, detail={"error": error})
    if not code or not state:
        raise HTTPException(status_code=400, detail="missing_code_or_state")

    signing_key = _get_setting("OAUTH_STATE_SIGNING_KEY", integration_id="oauth_global")
    if not signing_key:
        raise HTTPException(status_code=500, detail="missing_OAUTH_STATE_SIGNING_KEY")

    payload = _state_verify(state, signing_key)
    tenant_id = str(payload.get("tenant_id", "") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="missing_tenant_id_in_state")

    client_id = _get_setting("DROPBOX_APP_KEY", integration_id="oauth_dropbox")
    client_secret = _get_setting("DROPBOX_APP_SECRET", integration_id="oauth_dropbox")
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="missing_DROPBOX_APP_SECRET")

    redirect_uri = _redirect_uri(request, prov)

    resp = requests.post(
        DROPBOX_TOKEN_URL,
        data={
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
        auth=(client_id, client_secret),
        timeout=30,
    )

    if resp.status_code >= 400:
        raise HTTPException(status_code=400, detail={"token_exchange_failed": resp.text})

    token_json: Any = resp.json()
    if not isinstance(token_json, dict):
        raise HTTPException(status_code=500, detail="invalid_token_response")

    try:
        from platform.infra.adapters.tenant_credentials_csv import TenantCredentialsStoreCsv

        store = TenantCredentialsStoreCsv(repo_root=_repo_root(), tenants_root=_repo_root() / "tenants")
        store.upsert_integration(
            tenant_id=tenant_id,
            provider=prov,
            integration={
                "status": "active",
                "oauth": {
                    "token_url": DROPBOX_TOKEN_URL,
                },
                "token": token_json,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail={"store_write_failed": str(e)})

    return JSONResponse(
        {
            "status": "ok",
            "tenant_id": tenant_id,
            "provider": prov,
            "stored": True,
        }
    )
