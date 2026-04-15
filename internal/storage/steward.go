package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// DB returns the underlying *sql.DB, used by the migration runner.
func (s *PostgresStorage) DB() *sql.DB {
	return s.db.DB
}

// FetchUnsyncedRecords returns up to limit request logs not yet synced to Butler
// for the given org, ordered by created_at ascending.
func (s *PostgresStorage) FetchUnsyncedRecords(ctx context.Context, orgID uuid.UUID, limit int) ([]stewardclient.MetadataRecord, error) {
	const q = `
		SELECT
			id, majordomo_api_key_id, proxy_key_id,
			provider_api_key_hash, provider_api_key_alias,
			provider, model, request_path, request_method,
			requested_at, responded_at, response_time_ms,
			input_tokens, output_tokens, cached_tokens, cache_creation_tokens,
			input_cost, output_cost, total_cost,
			status_code, error_message,
			raw_metadata, indexed_metadata,
			body_s3_key, model_alias_found, org_id
		FROM llm_requests
		WHERE synced_to_butler = false AND org_id = $2
		ORDER BY created_at ASC
		LIMIT $1`

	rows, err := s.db.QueryContext(ctx, q, limit, orgID)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced records: %w", err)
	}
	defer rows.Close()

	var records []stewardclient.MetadataRecord
	for rows.Next() {
		var r stewardclient.MetadataRecord
		var rawMetaJSON, idxMetaJSON []byte

		if err := rows.Scan(
			&r.ID, &r.MajordomoAPIKeyID, &r.ProxyKeyID,
			&r.ProviderAPIKeyHash, &r.ProviderAPIKeyAlias,
			&r.Provider, &r.Model, &r.RequestPath, &r.RequestMethod,
			&r.RequestedAt, &r.RespondedAt, &r.ResponseTimeMS,
			&r.InputTokens, &r.OutputTokens, &r.CachedTokens, &r.CacheCreationTokens,
			&r.InputCost, &r.OutputCost, &r.TotalCost,
			&r.StatusCode, &r.ErrorMessage,
			&rawMetaJSON, &idxMetaJSON,
			&r.BodyS3Key, &r.ModelAliasFound, &r.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan record: %w", err)
		}

		if len(rawMetaJSON) > 0 {
			_ = json.Unmarshal(rawMetaJSON, &r.RawMetadata)
		}
		if len(idxMetaJSON) > 0 {
			_ = json.Unmarshal(idxMetaJSON, &r.IndexedMetadata)
		}

		records = append(records, r)
	}

	return records, rows.Err()
}

// MarkSynced marks the given request IDs as synced to Butler.
func (s *PostgresStorage) MarkSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	// Build a parameterized IN clause.
	args := make([]interface{}, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	q := fmt.Sprintf(
		`UPDATE llm_requests SET synced_to_butler = true WHERE id IN (%s)`,
		joinStrings(placeholders, ","),
	)

	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func joinStrings(ss []string, sep string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

// UpsertAPIKeys inserts or updates API keys received from Butler's key-sync endpoint.
func (s *PostgresStorage) UpsertAPIKeys(ctx context.Context, keys []stewardclient.APIKeyRecord) error {
	const q = `
		INSERT INTO api_keys (
			id, key_hash, name, description, is_active, org_id,
			created_at, revoked_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		ON CONFLICT (id) DO UPDATE SET
			key_hash    = EXCLUDED.key_hash,
			name        = EXCLUDED.name,
			description = EXCLUDED.description,
			is_active   = EXCLUDED.is_active,
			org_id      = EXCLUDED.org_id,
			revoked_at  = EXCLUDED.revoked_at,
			updated_at  = EXCLUDED.updated_at`

	for i := range keys {
		k := &keys[i]
		if _, err := s.db.ExecContext(ctx, q,
			k.ID, k.KeyHash, k.Name, k.Description, k.IsActive, k.OrgID,
			k.CreatedAt, k.RevokedAt, k.UpdatedAt,
		); err != nil {
			return fmt.Errorf("upsert api key %s: %w", k.ID, err)
		}
	}

	return nil
}

// ── Registered Orgs ───────────────────────────────────────────────────────────

// RegisterOrg upserts a registered org record (org_id is the primary key).
// Calling this again with the same org_id updates the name, butler_url, and token.
func (s *PostgresStorage) RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error {
	const q = `
		INSERT INTO registered_orgs (org_id, name, butler_url, token_encrypted)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (org_id) DO UPDATE SET
			name            = EXCLUDED.name,
			butler_url      = EXCLUDED.butler_url,
			token_encrypted = EXCLUDED.token_encrypted`

	_, err := s.db.ExecContext(ctx, q, orgID, name, butlerURL, tokenEncrypted)
	if err != nil {
		return fmt.Errorf("register org: %w", err)
	}
	return nil
}

// ListRegisteredOrgs returns all registered orgs ordered by created_at ascending.
func (s *PostgresStorage) ListRegisteredOrgs(ctx context.Context) ([]*RegisteredOrg, error) {
	const q = `
		SELECT org_id, name, butler_url, token_encrypted, created_at
		FROM registered_orgs
		ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, q)
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
func (s *PostgresStorage) RemoveRegisteredOrg(ctx context.Context, orgID uuid.UUID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM registered_orgs WHERE org_id = $1`, orgID)
	if err != nil {
		return fmt.Errorf("remove registered org: %w", err)
	}
	return nil
}
