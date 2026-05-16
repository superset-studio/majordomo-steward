-- Steward migration 008: replay and eval execution tables.
--
-- replay_runs / eval_runs / eval_sets / eval_set_items are received from Butler
-- via downstream sync. replay_results, eval_results, eval_result_scores, and
-- mutable run state (status, summary metrics) are produced locally by the worker
-- and synced upstream to Butler. Rows pending upstream sync have synced_at NULL.
--
-- No FKs to users/organizations: Steward stores owner UUIDs only, mirroring
-- cloud_storage_configs. FKs to local tables (llm_requests, replay_runs, etc.)
-- are kept and use explicit ON DELETE CASCADE where appropriate.

-- Replay

CREATE TABLE replay_runs (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                 UUID NOT NULL,
    org_id                  UUID,
    status                  VARCHAR(20) NOT NULL DEFAULT 'pending',
    error_message           TEXT,
    source_api_key_id       UUID,
    source_provider         VARCHAR(100),
    source_model            VARCHAR(100),
    source_start            TIMESTAMPTZ,
    source_end              TIMESTAMPTZ,
    source_metadata         JSONB,
    source_limit            INT NOT NULL DEFAULT 50,
    target_provider         VARCHAR(100) NOT NULL,
    target_model            VARCHAR(100) NOT NULL,
    judge_enabled           BOOLEAN NOT NULL DEFAULT false,
    judge_provider          VARCHAR(100),
    judge_model             VARCHAR(100),
    total_requests          INT,
    exact_matches           INT,
    judge_equivalent        INT,
    divergent               INT,
    original_total_cost     NUMERIC(12,8),
    replay_total_cost       NUMERIC(12,8),
    original_avg_latency_ms INT,
    replay_avg_latency_ms   INT,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    synced_at               TIMESTAMPTZ
);

CREATE INDEX idx_replay_runs_org_status ON replay_runs(org_id, status, created_at DESC)
    WHERE org_id IS NOT NULL;
CREATE INDEX idx_replay_runs_pending ON replay_runs(status) WHERE status = 'pending';
CREATE INDEX idx_replay_runs_unsynced ON replay_runs(updated_at)
    WHERE synced_at IS NULL OR synced_at < updated_at;

CREATE TABLE replay_results (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    replay_run_id           UUID NOT NULL REFERENCES replay_runs(id) ON DELETE CASCADE,
    source_request_id       UUID NOT NULL REFERENCES llm_requests(id) ON DELETE CASCADE,
    original_provider       VARCHAR(100) NOT NULL,
    original_model          VARCHAR(100) NOT NULL,
    original_cost           NUMERIC(12,8) NOT NULL,
    original_latency_ms     INT NOT NULL,
    original_input_tokens   INT NOT NULL,
    original_output_tokens  INT NOT NULL,
    replay_response         TEXT,
    replay_cost             NUMERIC(12,8),
    replay_latency_ms       INT,
    replay_input_tokens     INT,
    replay_output_tokens    INT,
    exact_match             BOOLEAN,
    judge_equivalent        BOOLEAN,
    judge_reason            TEXT,
    error_message           TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    synced_at               TIMESTAMPTZ
);

CREATE INDEX idx_replay_results_run ON replay_results(replay_run_id, created_at);
CREATE INDEX idx_replay_results_unsynced ON replay_results(created_at) WHERE synced_at IS NULL;

-- Eval system

CREATE TABLE eval_sets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL,
    org_id      UUID,
    name        VARCHAR(255) NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_eval_sets_org ON eval_sets(org_id, created_at DESC) WHERE org_id IS NOT NULL;

CREATE TABLE eval_set_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eval_set_id UUID NOT NULL REFERENCES eval_sets(id) ON DELETE CASCADE,
    request_id  UUID NOT NULL REFERENCES llm_requests(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (eval_set_id, request_id)
);

CREATE INDEX idx_eval_set_items_set ON eval_set_items(eval_set_id);
CREATE INDEX idx_eval_set_items_request ON eval_set_items(request_id);

CREATE TABLE eval_runs (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                 UUID NOT NULL,
    org_id                  UUID,
    eval_set_id             UUID NOT NULL REFERENCES eval_sets(id) ON DELETE CASCADE,
    status                  VARCHAR(20) NOT NULL DEFAULT 'pending',
    error_message           TEXT,
    target_provider         VARCHAR(100) NOT NULL,
    target_model            VARCHAR(100) NOT NULL,
    evaluators              JSONB NOT NULL DEFAULT '[]',
    total_requests          INT,
    successful_requests     INT,
    failed_requests         INT,
    original_total_cost     NUMERIC(12,8),
    replay_total_cost       NUMERIC(12,8),
    judge_total_cost        NUMERIC(12,8),
    original_avg_latency_ms INT,
    replay_avg_latency_ms   INT,
    evaluator_summary       JSONB,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    synced_at               TIMESTAMPTZ
);

CREATE INDEX idx_eval_runs_org_status ON eval_runs(org_id, status, created_at DESC)
    WHERE org_id IS NOT NULL;
CREATE INDEX idx_eval_runs_pending ON eval_runs(status) WHERE status = 'pending';
CREATE INDEX idx_eval_runs_eval_set ON eval_runs(eval_set_id);
CREATE INDEX idx_eval_runs_unsynced ON eval_runs(updated_at)
    WHERE synced_at IS NULL OR synced_at < updated_at;

CREATE TABLE eval_results (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eval_run_id             UUID NOT NULL REFERENCES eval_runs(id) ON DELETE CASCADE,
    source_request_id       UUID NOT NULL REFERENCES llm_requests(id) ON DELETE CASCADE,
    original_provider       VARCHAR(100) NOT NULL,
    original_model          VARCHAR(100) NOT NULL,
    original_cost           NUMERIC(12,8) NOT NULL,
    original_latency_ms     INT NOT NULL,
    original_input_tokens   INT NOT NULL,
    original_output_tokens  INT NOT NULL,
    replay_response         TEXT,
    replay_cost             NUMERIC(12,8),
    replay_latency_ms       INT,
    replay_input_tokens     INT,
    replay_output_tokens    INT,
    error_message           TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    synced_at               TIMESTAMPTZ
);

CREATE INDEX idx_eval_results_run ON eval_results(eval_run_id, created_at);
CREATE INDEX idx_eval_results_unsynced ON eval_results(created_at) WHERE synced_at IS NULL;

CREATE TABLE eval_result_scores (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eval_result_id UUID NOT NULL REFERENCES eval_results(id) ON DELETE CASCADE,
    evaluator_name VARCHAR(100) NOT NULL,
    score          NUMERIC(10,4) NOT NULL,
    reason         TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    synced_at      TIMESTAMPTZ,
    UNIQUE (eval_result_id, evaluator_name)
);

CREATE INDEX idx_eval_result_scores_result ON eval_result_scores(eval_result_id);
CREATE INDEX idx_eval_result_scores_name ON eval_result_scores(evaluator_name);
CREATE INDEX idx_eval_result_scores_unsynced ON eval_result_scores(created_at) WHERE synced_at IS NULL;
