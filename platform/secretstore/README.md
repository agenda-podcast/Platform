# Encrypted Secretstore (repository-safe)

This repository stores module and integration configuration in a single encrypted JSON file committed to the repository:

- `platform/secretstore/secretstore.json.gpg` (encrypted, safe to commit)
- `platform/secretstore/secretstore.template.json` (generated template, safe to commit)
- Plaintext `platform/secretstore/secretstore.json` must not exist in the repo

The decryption passphrase is provided at runtime via GitHub Actions secrets.

## Secretstore structure

Secretstore is organized into two top-level namespaces.

### 1) `modules.<module_id>` blocks

Used to provide module-scoped environment variables during module execution.

Each module block supports:

- `secrets`: secret values (tokens, API keys, passwords)
- `vars`: non-secret configuration values

During execution, the orchestrator loads the store and injects the module block into the module process environment for that run.

### 2) `integrations.<integration_id>` blocks

Used for platform-level integrations that are not tied to a single module, for example:

- artifact stores (S3 adapters, local stores)
- publishers (GitHub Releases, local publish, future providers)
- OAuth application secrets used by the OAuth callback service
- tenant credentials store configuration (development adapters)

Each integration block supports:

- `secrets`
- `vars`

Code reference:
- `platform/secretstore/loader.py::env_for_module`
- `platform/secretstore/loader.py::env_for_integration`

## Canonical loading mechanism

Workflows follow this pattern:

1) `SECRETSTORE_PASSPHRASE` is provided as an Actions secret
2) The encrypted file is decrypted to a runtime-only path
3) The decrypted JSON is loaded into memory, and env maps are derived per module or integration block

The loader never prints secret values.

## Admin flow (local)

### 1) Regenerate the template (maintenance)

Maintenance regenerates `secretstore.template.json` using:

- the set of modules in `modules/`
- runtime profile integration requirements in `config/runtime_profile*.yml`
- optional feature detection (for example Dropbox delivery support)

```bash
python -m platform.cli maintenance
```

### 2) Create a plaintext working copy (local only)

```bash
gpg --decrypt --batch --yes --passphrase "$SECRETSTORE_PASSPHRASE" --output platform/secretstore/secretstore.json platform/secretstore/secretstore.json.gpg
```

Edit `platform/secretstore/secretstore.json` locally, then re-encrypt:

```bash
gpg --symmetric --cipher-algo AES256 --batch --yes --passphrase "$SECRETSTORE_PASSPHRASE" --output platform/secretstore/secretstore.json.gpg platform/secretstore/secretstore.json
```

Remove plaintext and confirm it is gone:

```bash
rm -f platform/secretstore/secretstore.json
test ! -f platform/secretstore/secretstore.json
```

## Maintenance responsibilities (template drift and guardrails)

Maintenance is responsible for:

- regenerating `secretstore.template.json` deterministically
- failing verification if required keys are missing from the encrypted store for the selected runtime profile
- failing verification if plaintext `secretstore.json` is present anywhere in the repository
