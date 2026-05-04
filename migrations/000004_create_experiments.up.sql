-- Local cache of experiment definitions synced from Butler.
-- No FKs to other local tables: these rows are owned by Butler.
CREATE TABLE experiments (
    id               UUID        NOT NULL PRIMARY KEY,
    org_id           UUID        NOT NULL,
    status           TEXT        NOT NULL,
    api_key_id       UUID,
    metadata_filters JSONB       NOT NULL DEFAULT '{}',
    sticky_key       TEXT,
    starts_at        TIMESTAMPTZ NOT NULL,
    ends_at          TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL,
    synced_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_experiments_org_id_status ON experiments(org_id, status);

CREATE TABLE experiment_arms (
    id            UUID    NOT NULL PRIMARY KEY,
    experiment_id UUID    NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    name          TEXT    NOT NULL,
    model         TEXT    NOT NULL,
    weight        INT     NOT NULL,
    is_control    BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_experiment_arms_experiment_id ON experiment_arms(experiment_id);
