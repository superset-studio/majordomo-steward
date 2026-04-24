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

// ProxyKeyStorage is the interface satisfied by ProxyKeyRepository.
type ProxyKeyStorage interface {
	CreateProxyKey(ctx context.Context, keyHash string, majordomoKeyID uuid.UUID, input *models.CreateProxyKeyInput) (*models.ProxyKey, error)
	GetProxyKeyByHash(ctx context.Context, keyHash string) (*models.ProxyKey, error)
	GetProxyKeyByID(ctx context.Context, id uuid.UUID) (*models.ProxyKey, error)
	ListProxyKeys(ctx context.Context, majordomoKeyID uuid.UUID) ([]*models.ProxyKey, error)
	RevokeProxyKey(ctx context.Context, id uuid.UUID) error
	UpdateProxyKeyLastUsed(ctx context.Context, id uuid.UUID) error
	SetProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string, encryptedKey string) error
	GetProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string) (*models.ProviderMapping, error)
	ListProviderMappings(ctx context.Context, proxyKeyID uuid.UUID) ([]*models.ProviderMapping, error)
	DeleteProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string) error
}

// ProxyKeyRepository handles all proxy key data access.
type ProxyKeyRepository struct {
	db *sqlx.DB
}

// NewProxyKeyRepository constructs a ProxyKeyRepository backed by the given database.
func NewProxyKeyRepository(db *sqlx.DB) *ProxyKeyRepository {
	return &ProxyKeyRepository{db: db}
}

const proxyKeyColumns = `id, key_hash, name, description, majordomo_api_key_id, is_active, created_at, revoked_at, last_used_at, request_count`

func (r *ProxyKeyRepository) CreateProxyKey(ctx context.Context, keyHash string, majordomoKeyID uuid.UUID, input *models.CreateProxyKeyInput) (*models.ProxyKey, error) {
	query := `
		INSERT INTO proxy_keys (key_hash, name, description, majordomo_api_key_id)
		VALUES ($1, $2, $3, $4)
		RETURNING ` + proxyKeyColumns

	var key models.ProxyKey
	if err := r.db.QueryRowxContext(ctx, query, keyHash, input.Name, input.Description, majordomoKeyID).StructScan(&key); err != nil {
		return nil, fmt.Errorf("create proxy key: %w", err)
	}
	return &key, nil
}

func (r *ProxyKeyRepository) GetProxyKeyByHash(ctx context.Context, keyHash string) (*models.ProxyKey, error) {
	query := `SELECT ` + proxyKeyColumns + ` FROM proxy_keys WHERE key_hash = $1`

	var key models.ProxyKey
	if err := r.db.GetContext(ctx, &key, query, keyHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get proxy key by hash: %w", err)
	}
	return &key, nil
}

func (r *ProxyKeyRepository) GetProxyKeyByID(ctx context.Context, id uuid.UUID) (*models.ProxyKey, error) {
	query := `SELECT ` + proxyKeyColumns + ` FROM proxy_keys WHERE id = $1`

	var key models.ProxyKey
	if err := r.db.GetContext(ctx, &key, query, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrProxyKeyNotFound
		}
		return nil, fmt.Errorf("get proxy key by id: %w", err)
	}
	return &key, nil
}

func (r *ProxyKeyRepository) ListProxyKeys(ctx context.Context, majordomoKeyID uuid.UUID) ([]*models.ProxyKey, error) {
	query := `SELECT ` + proxyKeyColumns + ` FROM proxy_keys WHERE majordomo_api_key_id = $1 ORDER BY created_at DESC`

	var keys []*models.ProxyKey
	if err := r.db.SelectContext(ctx, &keys, query, majordomoKeyID); err != nil {
		return nil, fmt.Errorf("list proxy keys: %w", err)
	}
	return keys, nil
}

func (r *ProxyKeyRepository) RevokeProxyKey(ctx context.Context, id uuid.UUID) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE proxy_keys SET is_active = false, revoked_at = $1 WHERE id = $2 AND is_active = true`,
		time.Now(), id)
	if err != nil {
		return fmt.Errorf("revoke proxy key: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("revoke proxy key rows affected: %w", err)
	}
	if n == 0 {
		return ErrProxyKeyNotFound
	}
	return nil
}

func (r *ProxyKeyRepository) UpdateProxyKeyLastUsed(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE proxy_keys SET last_used_at = $1, request_count = request_count + 1 WHERE id = $2`,
		time.Now(), id)
	if err != nil {
		return fmt.Errorf("update proxy key last used: %w", err)
	}
	return nil
}

func (r *ProxyKeyRepository) SetProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string, encryptedKey string) error {
	query := `
		INSERT INTO proxy_key_provider_mappings (proxy_key_id, provider, encrypted_key)
		VALUES ($1, $2, $3)
		ON CONFLICT (proxy_key_id, provider) DO UPDATE
		SET encrypted_key = EXCLUDED.encrypted_key, updated_at = now()`

	_, err := r.db.ExecContext(ctx, query, proxyKeyID, provider, encryptedKey)
	if err != nil {
		return fmt.Errorf("set provider mapping: %w", err)
	}
	return nil
}

func (r *ProxyKeyRepository) GetProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string) (*models.ProviderMapping, error) {
	query := `
		SELECT id, proxy_key_id, provider, encrypted_key, created_at, updated_at
		FROM proxy_key_provider_mappings
		WHERE proxy_key_id = $1 AND provider = $2`

	var mapping models.ProviderMapping
	if err := r.db.GetContext(ctx, &mapping, query, proxyKeyID, provider); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get provider mapping: %w", err)
	}
	return &mapping, nil
}

func (r *ProxyKeyRepository) ListProviderMappings(ctx context.Context, proxyKeyID uuid.UUID) ([]*models.ProviderMapping, error) {
	query := `
		SELECT id, proxy_key_id, provider, encrypted_key, created_at, updated_at
		FROM proxy_key_provider_mappings
		WHERE proxy_key_id = $1
		ORDER BY provider`

	var mappings []*models.ProviderMapping
	if err := r.db.SelectContext(ctx, &mappings, query, proxyKeyID); err != nil {
		return nil, fmt.Errorf("list provider mappings: %w", err)
	}
	return mappings, nil
}

func (r *ProxyKeyRepository) DeleteProviderMapping(ctx context.Context, proxyKeyID uuid.UUID, provider string) error {
	result, err := r.db.ExecContext(ctx,
		`DELETE FROM proxy_key_provider_mappings WHERE proxy_key_id = $1 AND provider = $2`,
		proxyKeyID, provider)
	if err != nil {
		return fmt.Errorf("delete provider mapping: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete provider mapping rows affected: %w", err)
	}
	if n == 0 {
		return ErrProviderMappingNotFound
	}
	return nil
}
