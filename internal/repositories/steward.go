package repositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
)

// RegisteredOrg holds a per-org butler connection record stored locally.
type RegisteredOrg struct {
	OrgID          uuid.UUID
	Name           string
	ButlerURL      string
	TokenEncrypted string
	CreatedAt      time.Time
}

// StewardRepository handles steward-specific data access: Butler sync, registered
// orgs, and API key upsert. It satisfies stewardclient.ReporterStore,
// stewardclient.KeySyncStore, and stewardclient.OrgRegistrar.
type StewardRepository struct {
	db *sqlx.DB
}

// NewStewardRepository constructs a StewardRepository backed by the given database.
func NewStewardRepository(db *sqlx.DB) *StewardRepository {
	return &StewardRepository{db: db}
}

// FetchUnsyncedRecords returns up to limit request logs not yet synced to Butler
// for the given org, ordered by created_at ascending.
func (r *StewardRepository) FetchUnsyncedRecords(ctx context.Context, orgID uuid.UUID, limit int) ([]stewardclient.MetadataRecord, error) {
	const q = `
		SELECT
			id, user_id, majordomo_api_key_id, proxy_key_id,
			provider_api_key_hash, provider_api_key_alias,
			provider, model, request_path, request_method,
			requested_at, responded_at, response_time_ms,
			input_tokens, output_tokens, cached_tokens, cache_creation_tokens,
			input_cost, output_cost, total_cost,
			status_code, error_message,
			raw_metadata, indexed_metadata,
			body_s3_key, model_alias_found, org_id,
			experiment_id, experiment_arm_id, original_model
		FROM llm_requests
		WHERE synced_to_butler = false AND org_id = $2
		ORDER BY created_at ASC
		LIMIT $1`

	rows, err := r.db.QueryContext(ctx, q, limit, orgID)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced records: %w", err)
	}
	defer rows.Close()

	var records []stewardclient.MetadataRecord
	for rows.Next() {
		var rec stewardclient.MetadataRecord
		var rawMetaJSON, idxMetaJSON []byte
		var providerAPIKeyHash, providerAPIKeyAlias sql.NullString

		if err := rows.Scan(
			&rec.ID, &rec.UserID, &rec.MajordomoAPIKeyID, &rec.ProxyKeyID,
			&providerAPIKeyHash, &providerAPIKeyAlias,
			&rec.Provider, &rec.Model, &rec.RequestPath, &rec.RequestMethod,
			&rec.RequestedAt, &rec.RespondedAt, &rec.ResponseTimeMS,
			&rec.InputTokens, &rec.OutputTokens, &rec.CachedTokens, &rec.CacheCreationTokens,
			&rec.InputCost, &rec.OutputCost, &rec.TotalCost,
			&rec.StatusCode, &rec.ErrorMessage,
			&rawMetaJSON, &idxMetaJSON,
			&rec.BodyS3Key, &rec.ModelAliasFound, &rec.OrgID,
			&rec.ExperimentID, &rec.ExperimentArmID, &rec.OriginalModel,
		); err != nil {
			return nil, fmt.Errorf("scan record: %w", err)
		}
		rec.ProviderAPIKeyHash = providerAPIKeyHash.String
		rec.ProviderAPIKeyAlias = providerAPIKeyAlias.String

		if len(rawMetaJSON) > 0 {
			_ = json.Unmarshal(rawMetaJSON, &rec.RawMetadata)
		}
		if len(idxMetaJSON) > 0 {
			_ = json.Unmarshal(idxMetaJSON, &rec.IndexedMetadata)
		}

		records = append(records, rec)
	}
	return records, rows.Err()
}

// CountUnsyncedRecords returns the number of request logs not yet synced to Butler
// for the given org.
func (r *StewardRepository) CountUnsyncedRecords(ctx context.Context, orgID uuid.UUID) (int, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM llm_requests WHERE synced_to_butler = false AND org_id = $1`,
		orgID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count unsynced records: %w", err)
	}
	return count, nil
}

// MarkSynced marks the given request IDs as synced to Butler.
func (r *StewardRepository) MarkSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	args := make([]interface{}, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	q := fmt.Sprintf(
		`UPDATE llm_requests SET synced_to_butler = true WHERE id IN (%s)`,
		strings.Join(placeholders, ","),
	)

	_, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("mark synced: %w", err)
	}
	return nil
}

// UpsertAPIKeys inserts or updates API keys received from Butler's key-sync endpoint.
func (r *StewardRepository) UpsertAPIKeys(ctx context.Context, keys []stewardclient.APIKeyRecord) error {
	const q = `
		INSERT INTO api_keys (
			id, key_hash, name, description, is_active, user_id, org_id,
			created_at, revoked_at, updated_at, deprecated_model_behavior
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT (id) DO UPDATE SET
			key_hash                  = EXCLUDED.key_hash,
			name                      = EXCLUDED.name,
			description               = EXCLUDED.description,
			is_active                 = EXCLUDED.is_active,
			user_id                   = EXCLUDED.user_id,
			org_id                    = EXCLUDED.org_id,
			revoked_at                = EXCLUDED.revoked_at,
			updated_at                = EXCLUDED.updated_at,
			deprecated_model_behavior = EXCLUDED.deprecated_model_behavior`

	for i := range keys {
		k := &keys[i]
		behavior := k.DeprecatedModelBehavior
		if behavior == "" {
			behavior = "passthrough"
		}
		if _, err := r.db.ExecContext(ctx, q,
			k.ID, k.KeyHash, k.Name, k.Description, k.IsActive, k.UserID, k.OrgID,
			k.CreatedAt, k.RevokedAt, k.UpdatedAt, behavior,
		); err != nil {
			return fmt.Errorf("upsert api key %s: %w", k.ID, err)
		}
	}
	return nil
}

// RegisterOrg upserts a registered org record.
func (r *StewardRepository) RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error {
	const q = `
		INSERT INTO registered_orgs (org_id, name, butler_url, token_encrypted)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (org_id) DO UPDATE SET
			name            = EXCLUDED.name,
			butler_url      = EXCLUDED.butler_url,
			token_encrypted = EXCLUDED.token_encrypted`

	if _, err := r.db.ExecContext(ctx, q, orgID, name, butlerURL, tokenEncrypted); err != nil {
		return fmt.Errorf("register org: %w", err)
	}
	return nil
}

// ListRegisteredOrgs returns all registered orgs ordered by created_at ascending.
func (r *StewardRepository) ListRegisteredOrgs(ctx context.Context) ([]*RegisteredOrg, error) {
	const q = `
		SELECT org_id, name, butler_url, token_encrypted, created_at
		FROM registered_orgs
		ORDER BY created_at ASC`

	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list registered orgs: %w", err)
	}
	defer rows.Close()

	var orgs []*RegisteredOrg
	for rows.Next() {
		var o RegisteredOrg
		if err := rows.Scan(&o.OrgID, &o.Name, &o.ButlerURL, &o.TokenEncrypted, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan registered org: %w", err)
		}
		orgs = append(orgs, &o)
	}
	return orgs, rows.Err()
}

// RemoveRegisteredOrg deletes the registered org record for the given orgID.
func (r *StewardRepository) RemoveRegisteredOrg(ctx context.Context, orgID uuid.UUID) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM registered_orgs WHERE org_id = $1`, orgID); err != nil {
		return fmt.Errorf("remove registered org: %w", err)
	}
	return nil
}
