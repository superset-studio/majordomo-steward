CREATE TABLE cloud_storage_configs (
    owner_id    UUID        NOT NULL,
    owner_type  TEXT        NOT NULL CHECK (owner_type IN ('user', 'org')),
    provider    TEXT        NOT NULL,
    s3_bucket                      TEXT,
    s3_region                      TEXT,
    s3_endpoint                    TEXT,
    s3_access_key_id_encrypted     TEXT,
    s3_secret_access_key_encrypted TEXT,
    gcs_bucket                     TEXT,
    gcs_project_id                 TEXT,
    gcs_credentials_json_encrypted TEXT,
    synced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, owner_type)
);
