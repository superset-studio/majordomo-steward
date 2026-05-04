ALTER TABLE llm_requests
    ADD COLUMN IF NOT EXISTS experiment_id     UUID,
    ADD COLUMN IF NOT EXISTS experiment_arm_id UUID,
    ADD COLUMN IF NOT EXISTS original_model    TEXT;

CREATE INDEX idx_llm_requests_experiment_id ON llm_requests(experiment_id)
    WHERE experiment_id IS NOT NULL;
