-- Production schema draft (PostgreSQL)
-- Milestone C1: tenant credentials + published artifacts persistence.
--
-- Conventions:
--   - created_at / updated_at are UTC timestamps.
--   - metadata_json is optional JSONB for forward compatibility.
--   - Secrets are stored encrypted-at-rest at the application layer. The DB stores
--     ciphertext and associated crypto metadata.

BEGIN;

-- -----------------------------------------------------------------------------
-- Tenant integrations / credentials
-- -----------------------------------------------------------------------------
-- One row per (tenant_id, provider). Providers are stable identifiers such as:
--   "email_smtp", "dropbox", "gdrive", "s3", "github", "slack"
CREATE TABLE IF NOT EXISTS tenant_integrations (
    integration_id          UUID PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    provider                TEXT NOT NULL,
    display_name            TEXT NOT NULL DEFAULT '',
    status                  TEXT NOT NULL DEFAULT 'ACTIVE',

    -- Non-secret configuration (for example: host, port, region, bucket, from_email)
    config_json             JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Encrypted secret payload (application-encrypted)
    secret_ciphertext       BYTEA NOT NULL,
    secret_kms_key_id       TEXT NOT NULL DEFAULT '',
    secret_alg              TEXT NOT NULL DEFAULT 'AES-256-GCM',
    secret_iv               BYTEA NOT NULL,
    secret_tag              BYTEA NOT NULL,

    -- Rotation + auditing helpers
    secret_version          INTEGER NOT NULL DEFAULT 1,
    rotated_at              TIMESTAMPTZ NULL,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    metadata_json           JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT tenant_integrations_tenant_provider_uq UNIQUE (tenant_id, provider)
);

CREATE INDEX IF NOT EXISTS tenant_integrations_tenant_idx
    ON tenant_integrations (tenant_id);

CREATE INDEX IF NOT EXISTS tenant_integrations_provider_idx
    ON tenant_integrations (provider);

CREATE INDEX IF NOT EXISTS tenant_integrations_status_idx
    ON tenant_integrations (status);

-- -----------------------------------------------------------------------------
-- Published artifacts
-- -----------------------------------------------------------------------------
-- Records what was published for a purchased deliverable and where it was published.
-- The same logical publish should be idempotent via idempotency_key.
CREATE TABLE IF NOT EXISTS published_artifacts (
    published_artifact_id   UUID PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    work_order_id           TEXT NOT NULL,
    step_id                 TEXT NOT NULL,
    module_id               TEXT NOT NULL,
    deliverable_id          TEXT NOT NULL,

    artifact_key            TEXT NOT NULL,
    artifact_uri            TEXT NOT NULL,
    content_type            TEXT NOT NULL DEFAULT 'application/zip',

    sha256                  TEXT NOT NULL DEFAULT '',
    bytes_size              BIGINT NOT NULL DEFAULT 0,

    status                  TEXT NOT NULL DEFAULT 'PUBLISHED',
    idempotency_key         TEXT NOT NULL,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata_json           JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT published_artifacts_idem_uq UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS published_artifacts_workorder_idx
    ON published_artifacts (tenant_id, work_order_id);

CREATE INDEX IF NOT EXISTS published_artifacts_step_idx
    ON published_artifacts (tenant_id, work_order_id, step_id);

CREATE INDEX IF NOT EXISTS published_artifacts_module_idx
    ON published_artifacts (module_id);

CREATE INDEX IF NOT EXISTS published_artifacts_created_at_idx
    ON published_artifacts (created_at);

COMMIT;
