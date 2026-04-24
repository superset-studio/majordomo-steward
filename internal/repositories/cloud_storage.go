package repositories

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// CloudStorageConfigStore is the interface satisfied by CloudStorageRepository.
// proxy.Handler uses GetCloudStorageConfig; stewardclient.CloudStorageSyncer uses UpsertCloudStorageConfigs.
type CloudStorageConfigStore interface {
	GetCloudStorageConfig(ctx context.Context, ownerID uuid.UUID) (*models.CloudStorageRecord, error)
	UpsertCloudStorageConfigs(ctx context.Context, records []models.CloudStorageRecord) error
}

// CloudStorageRepository handles cloud storage config data access.
type CloudStorageRepository struct {
	db *sqlx.DB
}

// NewCloudStorageRepository constructs a CloudStorageRepository backed by the given database.
func NewCloudStorageRepository(db *sqlx.DB) *CloudStorageRepository {
	return &CloudStorageRepository{db: db}
}

// GetCloudStorageConfig returns the cloud storage config for the given owner,
// or nil, nil if none is stored.
func (r *CloudStorageRepository) GetCloudStorageConfig(ctx context.Context, ownerID uuid.UUID) (*models.CloudStorageRecord, error) {
	const q = `
		SELECT owner_id, owner_type, provider,
		       s3_bucket, s3_region, s3_endpoint,
		       s3_access_key_id_encrypted, s3_secret_access_key_encrypted,
		       gcs_bucket, gcs_project_id, gcs_credentials_json_encrypted,
		       synced_at
		FROM cloud_storage_configs
		WHERE owner_id = $1`

	var rec models.CloudStorageRecord
	err := r.db.QueryRowContext(ctx, q, ownerID).Scan(
		&rec.OwnerID, &rec.OwnerType, &rec.Provider,
		&rec.S3Bucket, &rec.S3Region, &rec.S3Endpoint,
		&rec.S3AccessKeyIDEncrypted, &rec.S3SecretAccessKeyEncrypted,
		&rec.GCSBucket, &rec.GCSProjectID, &rec.GCSCredentialsJSONEncrypted,
		&rec.SyncedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cloud storage config: %w", err)
	}
	return &rec, nil
}

// UpsertCloudStorageConfigs upserts cloud storage configs into local storage.
func (r *CloudStorageRepository) UpsertCloudStorageConfigs(ctx context.Context, records []models.CloudStorageRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
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
	for _, rec := range records {
		if _, err := tx.ExecContext(ctx, q,
			rec.OwnerID, rec.OwnerType, rec.Provider,
			rec.S3Bucket, rec.S3Region, rec.S3Endpoint,
			rec.S3AccessKeyIDEncrypted, rec.S3SecretAccessKeyEncrypted,
			rec.GCSBucket, rec.GCSProjectID, rec.GCSCredentialsJSONEncrypted,
			now,
		); err != nil {
			return fmt.Errorf("upsert cloud storage config for owner %s: %w", rec.OwnerID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert cloud storage configs: %w", err)
	}
	return nil
}
