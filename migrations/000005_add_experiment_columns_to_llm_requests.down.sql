DROP INDEX IF EXISTS idx_llm_requests_experiment_id;

ALTER TABLE llm_requests
    DROP COLUMN IF EXISTS original_model,
    DROP COLUMN IF EXISTS experiment_arm_id,
    DROP COLUMN IF EXISTS experiment_id;
