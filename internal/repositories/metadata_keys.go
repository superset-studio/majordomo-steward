package repositories

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// MetadataKeyStorage is the interface satisfied by MetadataKeyRepository.
type MetadataKeyStorage interface {
	ListMetadataKeys(ctx context.Context, userID uuid.UUID) ([]*models.MetadataKey, error)
	ActivateMetadataKey(ctx context.Context, apiKeyID uuid.UUID, keyName string) error
	DeactivateMetadataKey(ctx context.Context, apiKeyID uuid.UUID, keyName string) error
	UpdateMetadataKeyDisplayName(ctx context.Context, apiKeyID uuid.UUID, keyName string, displayName *string) error
}

// MetadataKeyRepository handles metadata key data access.
// It holds a reference to ActiveKeysCache so that activate/deactivate operations
// can immediately invalidate the cache.
type MetadataKeyRepository struct {
	db             *sqlx.DB
	activeKeyCache *ActiveKeysCache
}

// NewMetadataKeyRepository constructs a MetadataKeyRepository.
func NewMetadataKeyRepository(db *sqlx.DB, cache *ActiveKeysCache) *MetadataKeyRepository {
	return &MetadataKeyRepository{db: db, activeKeyCache: cache}
}

const metadataKeyColumns = `mk.majordomo_api_key_id, mk.key_name, mk.display_name, mk.key_type, mk.is_required, mk.is_active, mk.activated_at, mk.request_count, mk.last_seen_at, mk.approx_cardinality, mk.created_at`

func (r *MetadataKeyRepository) ListMetadataKeys(ctx context.Context, userID uuid.UUID) ([]*models.MetadataKey, error) {
	query := `
		SELECT ` + metadataKeyColumns + `
		FROM llm_requests_metadata_keys mk
		JOIN api_keys ak ON ak.id = mk.majordomo_api_key_id
		WHERE ak.user_id = $1
		ORDER BY mk.request_count DESC`

	var keys []*models.MetadataKey
	if err := r.db.SelectContext(ctx, &keys, query, userID); err != nil {
		return nil, fmt.Errorf("list metadata keys: %w", err)
	}
	return keys, nil
}

// ListActiveMetadataKeysByAPIKey returns active metadata key names for a specific API key.
func (r *MetadataKeyRepository) ListActiveMetadataKeysByAPIKey(ctx context.Context, apiKeyID uuid.UUID) ([]string, error) {
	query := `SELECT key_name FROM llm_requests_metadata_keys WHERE majordomo_api_key_id = $1 AND is_active = true`

	var names []string
	if err := r.db.SelectContext(ctx, &names, query, apiKeyID); err != nil {
		return nil, fmt.Errorf("list active metadata keys: %w", err)
	}
	return names, nil
}

// ListAllActiveMetadataKeys returns all (api_key_id, key_name) pairs that are active.
func (r *MetadataKeyRepository) ListAllActiveMetadataKeys(ctx context.Context) ([]struct {
	APIKeyID uuid.UUID `db:"majordomo_api_key_id"`
	KeyName  string    `db:"key_name"`
}, error) {
	query := `SELECT majordomo_api_key_id, key_name FROM llm_requests_metadata_keys WHERE is_active = true`

	var pairs []struct {
		APIKeyID uuid.UUID `db:"majordomo_api_key_id"`
		KeyName  string    `db:"key_name"`
	}
	if err := r.db.SelectContext(ctx, &pairs, query); err != nil {
		return nil, fmt.Errorf("list all active metadata keys: %w", err)
	}
	return pairs, nil
}

func (r *MetadataKeyRepository) ActivateMetadataKey(ctx context.Context, apiKeyID uuid.UUID, keyName string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE llm_requests_metadata_keys SET is_active = true, activated_at = NOW() WHERE majordomo_api_key_id = $1 AND key_name = $2`,
		apiKeyID, keyName)
	if err != nil {
		return fmt.Errorf("activate metadata key: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("activate metadata key rows affected: %w", err)
	}
	if n == 0 {
		return ErrMetadataKeyNotFound
	}
	r.activeKeyCache.InvalidateAPIKey(apiKeyID)
	return nil
}

func (r *MetadataKeyRepository) DeactivateMetadataKey(ctx context.Context, apiKeyID uuid.UUID, keyName string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE llm_requests_metadata_keys SET is_active = false WHERE majordomo_api_key_id = $1 AND key_name = $2`,
		apiKeyID, keyName)
	if err != nil {
		return fmt.Errorf("deactivate metadata key: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("deactivate metadata key rows affected: %w", err)
	}
	if n == 0 {
		return ErrMetadataKeyNotFound
	}
	r.activeKeyCache.InvalidateAPIKey(apiKeyID)
	return nil
}

// BackfillIndexedMetadata copies values for a single active key from raw_metadata
// into indexed_metadata, processing in batches.
func (r *MetadataKeyRepository) BackfillIndexedMetadata(ctx context.Context, apiKeyID uuid.UUID, keyName string, batchSize int) (int64, error) {
	query := `
		UPDATE llm_requests
		SET indexed_metadata = COALESCE(indexed_metadata, '{}'::jsonb) || jsonb_build_object($2, raw_metadata->>$2)
		WHERE id IN (
			SELECT id FROM llm_requests
			WHERE majordomo_api_key_id = $1
				AND raw_metadata ? $2
				AND (indexed_metadata IS NULL OR NOT indexed_metadata ? $2)
			LIMIT $3
		)`

	total := int64(0)
	for {
		result, err := r.db.ExecContext(ctx, query, apiKeyID, keyName, batchSize)
		if err != nil {
			return total, fmt.Errorf("backfill batch for key %q: %w", keyName, err)
		}
		n, _ := result.RowsAffected()
		total += n
		if n > 0 {
			slog.Info("backfill indexed_metadata progress", "api_key_id", apiKeyID, "key", keyName, "batch", n, "total", total)
		}
		if n < int64(batchSize) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return total, nil
}

// CleanupIndexedMetadata removes a key from indexed_metadata for all requests
// belonging to an API key, processing in batches.
func (r *MetadataKeyRepository) CleanupIndexedMetadata(ctx context.Context, apiKeyID uuid.UUID, keyName string, batchSize int) (int64, error) {
	query := `
		UPDATE llm_requests
		SET indexed_metadata = indexed_metadata - $2
		WHERE id IN (
			SELECT id FROM llm_requests
			WHERE majordomo_api_key_id = $1
				AND indexed_metadata ? $2
			LIMIT $3
		)`

	total := int64(0)
	for {
		result, err := r.db.ExecContext(ctx, query, apiKeyID, keyName, batchSize)
		if err != nil {
			return total, fmt.Errorf("cleanup batch for key %q: %w", keyName, err)
		}
		n, _ := result.RowsAffected()
		total += n
		if n > 0 {
			slog.Info("cleanup indexed_metadata progress", "api_key_id", apiKeyID, "key", keyName, "batch", n, "total", total)
		}
		if n < int64(batchSize) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return total, nil
}

func (r *MetadataKeyRepository) UpdateMetadataKeyDisplayName(ctx context.Context, apiKeyID uuid.UUID, keyName string, displayName *string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE llm_requests_metadata_keys SET display_name = $3 WHERE majordomo_api_key_id = $1 AND key_name = $2`,
		apiKeyID, keyName, displayName)
	if err != nil {
		return fmt.Errorf("update metadata key display name: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update metadata key display name rows affected: %w", err)
	}
	if n == 0 {
		return ErrMetadataKeyNotFound
	}
	return nil
}
