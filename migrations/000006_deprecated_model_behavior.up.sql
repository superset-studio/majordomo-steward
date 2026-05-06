-- Add per-key deprecated model behavior and tracking column for redirect events.

ALTER TABLE api_keys
    ADD COLUMN deprecated_model_behavior VARCHAR(20) NOT NULL DEFAULT 'passthrough';

ALTER TABLE llm_requests
    ADD COLUMN deprecated_model_redirected BOOLEAN NOT NULL DEFAULT false;
