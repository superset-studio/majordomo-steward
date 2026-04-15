-- Majordomo Steward: initial schema
-- Only tables required for proxying and local request logging.
-- Management tables (users, orgs, replay, eval) live in Butler.

-- API Keys (synced from Butler; no FK to users/orgs — those live in Butler)
CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash        VARCHAR(64) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    org_id          UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ,
    request_count   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash) WHERE is_active = true;
CREATE INDEX idx_api_keys_org_updated_at ON api_keys(org_id, updated_at);

-- Proxy Keys (synced from Butler)
CREATE TABLE proxy_keys (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash             VARCHAR(64) NOT NULL UNIQUE,
    name                 VARCHAR(255) NOT NULL,
    description          TEXT,
    majordomo_api_key_id UUID NOT NULL REFERENCES api_keys(id),
    is_active            BOOLEAN NOT NULL DEFAULT true,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at           TIMESTAMPTZ,
    last_used_at         TIMESTAMPTZ,
    request_count        BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_proxy_keys_hash ON proxy_keys(key_hash) WHERE is_active = true;
CREATE INDEX idx_proxy_keys_majordomo_key ON proxy_keys(majordomo_api_key_id);

-- Proxy Key Provider Mappings (encrypted provider credentials)
CREATE TABLE proxy_key_provider_mappings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proxy_key_id  UUID NOT NULL REFERENCES proxy_keys(id) ON DELETE CASCADE,
    provider      VARCHAR(100) NOT NULL,
    encrypted_key TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(proxy_key_id, provider)
);

-- LLM Request Logs (local copy; metadata is batched to Butler async)
CREATE TABLE llm_requests (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    majordomo_api_key_id    UUID REFERENCES api_keys(id),
    proxy_key_id            UUID REFERENCES proxy_keys(id),
    provider_api_key_hash   VARCHAR(64),
    provider_api_key_alias  VARCHAR(255),
    provider                VARCHAR(100) NOT NULL,
    model                   VARCHAR(100) NOT NULL,
    request_path            TEXT NOT NULL,
    request_method          TEXT NOT NULL,
    requested_at            TIMESTAMPTZ NOT NULL,
    responded_at            TIMESTAMPTZ NOT NULL,
    response_time_ms        INT NOT NULL,
    input_tokens            INT NOT NULL,
    output_tokens           INT NOT NULL,
    cached_tokens           INT DEFAULT 0,
    cache_creation_tokens   INT DEFAULT 0,
    input_cost              NUMERIC(12, 8) NOT NULL,
    output_cost             NUMERIC(12, 8) NOT NULL,
    total_cost              NUMERIC(12, 8) NOT NULL,
    status_code             INT NOT NULL,
    error_message           TEXT,
    raw_metadata            JSONB,
    indexed_metadata        JSONB DEFAULT '{}',
    request_body            TEXT,
    response_body           TEXT,
    body_s3_key             TEXT,
    model_alias_found       BOOLEAN NOT NULL DEFAULT true,
    org_id                  UUID,
    created_at              TIMESTAMPTZ DEFAULT now(),
    -- Steward-only: tracks whether this record has been batched to Butler.
    synced_to_butler        BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_llm_requests_unsynced ON llm_requests(created_at ASC)
    WHERE synced_to_butler = false;

CREATE INDEX idx_llm_requests_key_time ON llm_requests(majordomo_api_key_id, requested_at DESC);
CREATE INDEX idx_llm_requests_indexed_metadata_gin ON llm_requests USING GIN (indexed_metadata);

-- Metadata Key Configuration (HyperLogLog state + active key cache)
CREATE TABLE llm_requests_metadata_keys (
    majordomo_api_key_id UUID NOT NULL REFERENCES api_keys(id),
    key_name             VARCHAR(255) NOT NULL,
    display_name         VARCHAR(255),
    key_type             VARCHAR(50) DEFAULT 'string',
    is_required          BOOLEAN DEFAULT false,
    is_active            BOOLEAN NOT NULL DEFAULT false,
    activated_at         TIMESTAMPTZ,
    request_count        BIGINT NOT NULL DEFAULT 0,
    last_seen_at         TIMESTAMPTZ,
    hll_state            BYTEA,
    approx_cardinality   INT NOT NULL DEFAULT 0,
    hll_updated_at       TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (majordomo_api_key_id, key_name)
);

CREATE INDEX idx_llm_requests_metadata_keys_active
    ON llm_requests_metadata_keys(majordomo_api_key_id) WHERE is_active = true;
