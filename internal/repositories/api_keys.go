package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// APIKeyStorage is the interface satisfied by APIKeyRepository.
type APIKeyStorage interface {
	CreateAPIKey(ctx context.Context, keyHash string, input *models.CreateAPIKeyInput) (*models.APIKey, error)
	GetAPIKeyByHash(ctx context.Context, keyHash string) (*models.APIKey, error)
	GetAPIKeyByID(ctx context.Context, id uuid.UUID) (*models.APIKey, error)
	ListAPIKeys(ctx context.Context) ([]*models.APIKey, error)
	UpdateAPIKey(ctx context.Context, id uuid.UUID, input *models.UpdateAPIKeyInput) (*models.APIKey, error)
	RevokeAPIKey(ctx context.Context, id uuid.UUID) error
	UpdateAPIKeyLastUsed(ctx context.Context, id uuid.UUID) error
	ListAPIKeysByUserID(ctx context.Context, userID uuid.UUID) ([]*models.APIKey, error)
	ListAPIKeysByOrgID(ctx context.Context, orgID uuid.UUID) ([]*models.APIKey, error)
}

// APIKeyRepository handles all API key data access.
type APIKeyRepository struct {
	db *sqlx.DB
}

// NewAPIKeyRepository constructs an APIKeyRepository backed by the given database.
func NewAPIKeyRepository(db *sqlx.DB) *APIKeyRepository {
	return &APIKeyRepository{db: db}
}

const apiKeyColumns = `id, key_hash, name, description, user_id, org_id, is_active, created_at, revoked_at, last_used_at, request_count`

func (r *APIKeyRepository) CreateAPIKey(ctx context.Context, keyHash string, input *models.CreateAPIKeyInput) (*models.APIKey, error) {
	query := `
		INSERT INTO api_keys (key_hash, name, description, user_id, org_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING ` + apiKeyColumns

	var key models.APIKey
	if err := r.db.QueryRowxContext(ctx, query, keyHash, input.Name, input.Description, input.UserID, input.OrgID).StructScan(&key); err != nil {
		return nil, fmt.Errorf("create api key: %w", err)
	}
	return &key, nil
}

func (r *APIKeyRepository) GetAPIKeyByHash(ctx context.Context, keyHash string) (*models.APIKey, error) {
	query := `SELECT ` + apiKeyColumns + ` FROM api_keys WHERE key_hash = $1`

	var key models.APIKey
	if err := r.db.GetContext(ctx, &key, query, keyHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get api key by hash: %w", err)
	}
	return &key, nil
}

func (r *APIKeyRepository) GetAPIKeyByID(ctx context.Context, id uuid.UUID) (*models.APIKey, error) {
	query := `SELECT ` + apiKeyColumns + ` FROM api_keys WHERE id = $1`

	var key models.APIKey
	if err := r.db.GetContext(ctx, &key, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("get api key by id: %w", err)
	}
	return &key, nil
}

func (r *APIKeyRepository) ListAPIKeys(ctx context.Context) ([]*models.APIKey, error) {
	query := `SELECT ` + apiKeyColumns + ` FROM api_keys ORDER BY created_at DESC`

	var keys []*models.APIKey
	if err := r.db.SelectContext(ctx, &keys, query); err != nil {
		return nil, fmt.Errorf("list api keys: %w", err)
	}
	return keys, nil
}

func (r *APIKeyRepository) UpdateAPIKey(ctx context.Context, id uuid.UUID, input *models.UpdateAPIKeyInput) (*models.APIKey, error) {
	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	if input.Name != nil {
		setClauses = append(setClauses, fmt.Sprintf("name = $%d", argIdx))
		args = append(args, *input.Name)
		argIdx++
	}
	if input.Description != nil {
		setClauses = append(setClauses, fmt.Sprintf("description = $%d", argIdx))
		args = append(args, *input.Description)
		argIdx++
	}

	if len(setClauses) == 0 {
		return r.GetAPIKeyByID(ctx, id)
	}

	query := "UPDATE api_keys SET "
	for i, clause := range setClauses {
		if i > 0 {
			query += ", "
		}
		query += clause
	}
	query += fmt.Sprintf(" WHERE id = $%d RETURNING ", argIdx) + apiKeyColumns
	args = append(args, id)

	var key models.APIKey
	if err := r.db.QueryRowxContext(ctx, query, args...).StructScan(&key); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAPIKeyNotFound
		}
		return nil, fmt.Errorf("update api key: %w", err)
	}
	return &key, nil
}

func (r *APIKeyRepository) RevokeAPIKey(ctx context.Context, id uuid.UUID) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE api_keys SET is_active = false, revoked_at = $1 WHERE id = $2 AND is_active = true`,
		time.Now(), id)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("revoke api key rows affected: %w", err)
	}
	if n == 0 {
		return ErrAPIKeyNotFound
	}
	return nil
}

func (r *APIKeyRepository) UpdateAPIKeyLastUsed(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE api_keys SET last_used_at = $1, request_count = request_count + 1 WHERE id = $2`,
		time.Now(), id)
	if err != nil {
		return fmt.Errorf("update api key last used: %w", err)
	}
	return nil
}

func (r *APIKeyRepository) ListAPIKeysByUserID(ctx context.Context, userID uuid.UUID) ([]*models.APIKey, error) {
	query := `SELECT ` + apiKeyColumns + ` FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC`

	var keys []*models.APIKey
	if err := r.db.SelectContext(ctx, &keys, query, userID); err != nil {
		return nil, fmt.Errorf("list api keys by user id: %w", err)
	}
	return keys, nil
}

func (r *APIKeyRepository) ListAPIKeysByOrgID(ctx context.Context, orgID uuid.UUID) ([]*models.APIKey, error) {
	query := `SELECT ` + apiKeyColumns + ` FROM api_keys WHERE org_id = $1 ORDER BY created_at DESC`

	var keys []*models.APIKey
	if err := r.db.SelectContext(ctx, &keys, query, orgID); err != nil {
		return nil, fmt.Errorf("list api keys by org id: %w", err)
	}
	return keys, nil
}
