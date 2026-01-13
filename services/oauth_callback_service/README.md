# OAuth callback service (skeleton)

This is a minimal FastAPI application that can handle OAuth authorization for tenant-owned destinations.

## Local run

1) Create a virtual environment and install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r services/oauth_callback_service/requirements.txt
```

2) Provide required settings.

You can use direct environment variables or the repo secretstore integration blocks.

### Direct environment variables

Required:
- OAUTH_STATE_SIGNING_KEY
- TOKEN_ENCRYPTION_KEY (or SECRETSTORE_PASSPHRASE)
- DROPBOX_APP_KEY
- DROPBOX_APP_SECRET

Optional:
- DROPBOX_SCOPES
- OAUTH_REDIRECT_BASE (public base URL, for example https://example.ngrok.io)
- PLATFORM_REPO_ROOT (defaults to current directory)

3) Start the server:

```bash
uvicorn services.oauth_callback_service.app:app --reload --port 8000
```

4) Start OAuth:

```bash
open "http://127.0.0.1:8000/start/dropbox?tenant_id=TENANT123"
```

5) After authorization, the callback stores the token in:

- tenants/<tenant_id>/integrations/tenant_integrations.csv
- tenants/<tenant_id>/integrations/tokens.gpg

## Notes

- The encrypted token storage uses symmetric GPG encryption (AES256). The passphrase is taken from TOKEN_ENCRYPTION_KEY, then SECRETSTORE_PASSPHRASE.
- This is a skeleton intended for local use. It is not a production deployment guide.
