# Encrypted Secretstore (Repository-safe)

## Goal
Store all module secrets and variables in a single JSON file *encrypted* in the repository, while keeping
the decryption passphrase in GitHub Secrets. This avoids:
- GitHub's case-insensitive secret names
- secret-count limits
- accidental exposure of plaintext secrets in repo history

## Files
- `platform/secretstore/secretstore.template.json` (safe to commit; placeholders only)
- `platform/secretstore/secretstore.json.gpg` (encrypted; safe to commit)
- plaintext `platform/secretstore/secretstore.json` MUST NOT exist in the repo

## Admin flow (local only)
1) Generate/refresh the template:
   `python scripts/secretstore_generate_template.py`
2) Copy template to a *local* plaintext file and fill values:
   `cp platform/secretstore/secretstore.template.json platform/secretstore/secretstore.json`
3) Encrypt plaintext to repo-safe file:
   `bash scripts/secretstore_encrypt_local.sh`
4) Confirm plaintext is removed:
   `test ! -f platform/secretstore/secretstore.json`
5) Commit/push ONLY:
   - `platform/secretstore/secretstore.template.json` (optional but recommended)
   - `platform/secretstore/secretstore.json.gpg`

## GitHub Actions runtime
- Store passphrase in GitHub Secret: `SECRETSTORE_PASSPHRASE`
- Decrypt `secretstore.json.gpg` into a runtime-only path (e.g. `runtime/secure/secretstore.json`)
- Orchestrator loads decrypted JSON and injects env vars per module manifest.

## Security guardrails
- CI fails if plaintext `platform/secretstore/secretstore.json` exists.
- Prefer storing `SECRETSTORE_PASSPHRASE` as an Environment Secret (with required reviewers) on protected branches.
