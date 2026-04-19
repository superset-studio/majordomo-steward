package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/models"
)

// CloudStorageConfigStore is the minimal interface the proxy.Handler needs
// to resolve per-owner cloud storage configs, and the CloudStorageSyncer needs
// to persist synced configs locally.
type CloudStorageConfigStore interface {
	// GetCloudStorageConfig returns the stored config for the given owner,
	// or nil, nil if no config exists.
	GetCloudStorageConfig(ctx context.Context, ownerID uuid.UUID) (*models.CloudStorageRecord, error)

	// UpsertCloudStorageConfigs upserts the given cloud storage configs into
	// local storage. Records are added or updated; nothing is deleted.
	// Deletions are handled by the event-driven sync redesign.
	UpsertCloudStorageConfigs(ctx context.Context, records []models.CloudStorageRecord) error
}

// GetCloudStorageConfig returns the cloud storage config for the given owner,
// or nil, nil if none is stored.
func (s *PostgresStorage) GetCloudStorageConfig(ctx context.Context, ownerID uuid.UUID) (*models.CloudStorageRecord, error) {
	const q = `
		SELECT owner_id, owner_type, provider,
		       s3_bucket, s3_region, s3_endpoint,
		       s3_access_key_id_encrypted, s3_secret_access_key_encrypted,
		       gcs_bucket, gcs_project_id, gcs_credentials_json_encrypted,
		       synced_at
		FROM cloud_storage_configs
		WHERE owner_id = $1`

	var r models.CloudStorageRecord
	err := s.db.QueryRowContext(ctx, q, ownerID).Scan(
		&r.OwnerID, &r.OwnerType, &r.Provider,
		&r.S3Bucket, &r.S3Region, &r.S3Endpoint,
		&r.S3AccessKeyIDEncrypted, &r.S3SecretAccessKeyEncrypted,
		&r.GCSBucket, &r.GCSProjectID, &r.GCSCredentialsJSONEncrypted,
		&r.SyncedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cloud storage config: %w", err)
	}
	return &r, nil
}

// UpsertCloudStorageConfigs upserts cloud storage configs into local storage.
// Each record is inserted or updated by (owner_id, owner_type). Nothing is
// deleted — removal is handled by the event-driven sync redesign.
func (s *PostgresStorage) UpsertCloudStorageConfigs(ctx context.Context, records []models.CloudStorageRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	const q = `
		INSERT INTO cloud_storage_configs (
			owner_id, owner_type, provider,
			s3_bucket, s3_region, s3_endpoint,
			s3_access_key_id_encrypted, s3_secret_access_key_encrypted,
			gcs_bucket, gcs_project_id, gcs_credentials_json_encrypted,
			synced_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (owner_id, owner_type) DO UPDATE SET
			provider                       = EXCLUDED.provider,
			s3_bucket                      = EXCLUDED.s3_bucket,
			s3_region                      = EXCLUDED.s3_region,
			s3_endpoint                    = EXCLUDED.s3_endpoint,
			s3_access_key_id_encrypted     = EXCLUDED.s3_access_key_id_encrypted,
			s3_secret_access_key_encrypted = EXCLUDED.s3_secret_access_key_encrypted,
			gcs_bucket                     = EXCLUDED.gcs_bucket,
			gcs_project_id                 = EXCLUDED.gcs_project_id,
			gcs_credentials_json_encrypted = EXCLUDED.gcs_credentials_json_encrypted,
			synced_at                      = EXCLUDED.synced_at`

	now := time.Now()
	for _, r := range records {
		if _, err := tx.ExecContext(ctx, q,
			r.OwnerID, r.OwnerType, r.Provider,
			r.S3Bucket, r.S3Region, r.S3Endpoint,
			r.S3AccessKeyIDEncrypted, r.S3SecretAccessKeyEncrypted,
			r.GCSBucket, r.GCSProjectID, r.GCSCredentialsJSONEncrypted,
			now,
		); err != nil {
			return fmt.Errorf("upsert cloud storage config for owner %s: %w", r.OwnerID, err)
		}
	}

	return tx.Commit()
}
