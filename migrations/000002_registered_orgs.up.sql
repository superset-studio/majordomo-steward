CREATE TABLE registered_orgs (
    org_id           UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    butler_url       TEXT NOT NULL,
    token_encrypted  TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
