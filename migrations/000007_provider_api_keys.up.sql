-- Steward migration 007: per-user/org encrypted provider API keys.
-- Synced down from Butler for use by the local replay/eval worker.
-- No FKs to users/organizations: Steward stores owner UUIDs only, mirroring
-- the cloud_storage_configs pattern.

CREATE TABLE provider_api_keys (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID,
    org_id        UUID,
    provider      VARCHAR(100) NOT NULL,
    encrypted_key TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (user_id IS NOT NULL AND org_id IS NULL) OR
        (user_id IS NULL AND org_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX idx_provider_api_keys_user_provider
    ON provider_api_keys(user_id, provider) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX idx_provider_api_keys_org_provider
    ON provider_api_keys(org_id, provider) WHERE org_id IS NOT NULL;
