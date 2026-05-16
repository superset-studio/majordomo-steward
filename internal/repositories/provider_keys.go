package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// ProviderKeyStorage is the interface satisfied by ProviderKeyRepository.
type ProviderKeyStorage interface {
	Upsert(ctx context.Context, key *models.ProviderAPIKey) error
	UpsertBatch(ctx context.Context, keys []*models.ProviderAPIKey) error
	GetProviderKey(ctx context.Context, userID *uuid.UUID, orgID *uuid.UUID, provider string) (*models.ProviderAPIKey, error)
}

// ProviderKeyRepository handles per-user/org encrypted provider API key access.
// Rows are synced down from Butler and consumed locally by the replay/eval worker.
type ProviderKeyRepository struct {
	db *sqlx.DB
}

// NewProviderKeyRepository constructs a ProviderKeyRepository backed by the given database.
func NewProviderKeyRepository(db *sqlx.DB) *ProviderKeyRepository {
	return &ProviderKeyRepository{db: db}
}

// Upsert inserts or updates a single provider API key, keyed by (user_id, provider)
// or (org_id, provider). Exactly one of UserID/OrgID must be set.
func (r *ProviderKeyRepository) Upsert(ctx context.Context, key *models.ProviderAPIKey) error {
	return upsertProviderKey(ctx, r.db, key)
}

// UpsertBatch upserts multiple provider keys atomically.
func (r *ProviderKeyRepository) UpsertBatch(ctx context.Context, keys []*models.ProviderAPIKey) error {
	if len(keys) == 0 {
		return nil
	}

	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, k := range keys {
		if err := upsertProviderKey(ctx, tx, k); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert provider keys: %w", err)
	}
	return nil
}

// GetProviderKey returns the full provider key record (including encrypted key)
// for the given owner and provider. Returns ErrProviderKeyNotFound when not found.
func (r *ProviderKeyRepository) GetProviderKey(ctx context.Context, userID *uuid.UUID, orgID *uuid.UUID, provider string) (*models.ProviderAPIKey, error) {
	var (
		query string
		args  []interface{}
	)
	if orgID != nil {
		query = `SELECT id, user_id, org_id, provider, encrypted_key, created_at, updated_at FROM provider_api_keys WHERE org_id = $1 AND provider = $2`
		args = []interface{}{*orgID, provider}
	} else {
		query = `SELECT id, user_id, org_id, provider, encrypted_key, created_at, updated_at FROM provider_api_keys WHERE user_id = $1 AND provider = $2`
		args = []interface{}{*userID, provider}
	}

	var key models.ProviderAPIKey
	if err := r.db.GetContext(ctx, &key, query, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrProviderKeyNotFound
		}
		return nil, fmt.Errorf("get provider key: %w", err)
	}
	return &key, nil
}

// upsertProviderKey runs the upsert against either a *sqlx.DB or *sqlx.Tx.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func upsertProviderKey(ctx context.Context, e execer, key *models.ProviderAPIKey) error {
	if key.UserID != nil {
		_, err := e.ExecContext(ctx, `
			INSERT INTO provider_api_keys (user_id, provider, encrypted_key, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (user_id, provider) WHERE user_id IS NOT NULL
			DO UPDATE SET encrypted_key = $3, updated_at = now()`,
			*key.UserID, key.Provider, key.EncryptedKey)
		if err != nil {
			return fmt.Errorf("upsert provider key for user: %w", err)
		}
		return nil
	}
	if key.OrgID == nil {
		return fmt.Errorf("upsert provider key: user_id and org_id both nil")
	}
	_, err := e.ExecContext(ctx, `
		INSERT INTO provider_api_keys (org_id, provider, encrypted_key, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (org_id, provider) WHERE org_id IS NOT NULL
		DO UPDATE SET encrypted_key = $3, updated_at = now()`,
		*key.OrgID, key.Provider, key.EncryptedKey)
	if err != nil {
		return fmt.Errorf("upsert provider key for org: %w", err)
	}
	return nil
}
