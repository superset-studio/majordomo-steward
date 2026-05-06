ALTER TABLE llm_requests
    DROP COLUMN deprecated_model_redirected;

ALTER TABLE api_keys
    DROP COLUMN deprecated_model_behavior;
