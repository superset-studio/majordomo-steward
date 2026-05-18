package stewardclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/config"
)

// APIKeyRecord is the API key representation returned by Butler's key-sync endpoint.
type APIKeyRecord struct {
	ID                      uuid.UUID  `json:"id"`
	KeyHash                 string     `json:"key_hash"`
	Name                    string     `json:"name"`
	Description             *string    `json:"description,omitempty"`
	IsActive                bool       `json:"is_active"`
	UserID                  *uuid.UUID `json:"user_id,omitempty"`
	OrgID                   *uuid.UUID `json:"org_id,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	RevokedAt               *time.Time `json:"revoked_at,omitempty"`
	UpdatedAt               time.Time  `json:"updated_at"`
	DeprecatedModelBehavior string     `json:"deprecated_model_behavior"`
}

// KeySyncStore is the minimal storage interface required by the key syncer.
type KeySyncStore interface {
	UpsertAPIKeys(ctx context.Context, keys []APIKeyRecord) error
}

// KeySyncer fetches changed API keys from Butler and upserts them locally.
//
// Stateless across goroutines except for the cursor (updated_after watermark),
// which is mutex-protected. The WorkTicker invokes Sync when a sync_api_keys
// job is received; this type does not manage its own ticker or goroutines.
type KeySyncer struct {
	cfg    config.StewardConfig
	store  KeySyncStore
	client *http.Client

	mu     sync.Mutex
	cursor time.Time
}

// NewKeySyncer constructs a KeySyncer with a zero cursor (first sync fetches all keys).
func NewKeySyncer(cfg config.StewardConfig, store KeySyncStore) *KeySyncer {
	return &KeySyncer{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// Sync fetches all API keys changed since the last cursor and upserts them.
// The cursor advances only on a successful upsert. The passed logger should
// already carry tick_id / org_id / job_id context.
func (ks *KeySyncer) Sync(ctx context.Context, logger *slog.Logger) error {
	ks.mu.Lock()
	cursor := ks.cursor
	ks.mu.Unlock()

	keys, newCursor, err := ks.fetch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(keys) == 0 {
		logger.Info("api key sync: no changes", "cursor", cursor)
		return nil
	}

	if err := ks.store.UpsertAPIKeys(ctx, keys); err != nil {
		return fmt.Errorf("upsert %d keys: %w", len(keys), err)
	}

	ks.mu.Lock()
	ks.cursor = newCursor
	ks.mu.Unlock()

	logger.Info("api key sync applied", "upserted", len(keys), "new_cursor", newCursor)
	return nil
}

func (ks *KeySyncer) fetch(ctx context.Context, cursor time.Time) ([]APIKeyRecord, time.Time, error) {
	const limit = 200
	var all []APIKeyRecord

	for {
		url := fmt.Sprintf("%s/api/v1/steward/api-keys?updated_after=%s&limit=%d",
			ks.cfg.ButlerBaseURL,
			cursor.UTC().Format(time.RFC3339Nano),
			limit,
		)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, cursor, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+ks.cfg.StewardToken)

		resp, err := ks.client.Do(req)
		if err != nil {
			return nil, cursor, fmt.Errorf("get keys: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, cursor, fmt.Errorf("butler returned %d", resp.StatusCode)
		}

		var page []APIKeyRecord
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, cursor, fmt.Errorf("decode response: %w", err)
		}
		resp.Body.Close()

		all = append(all, page...)
		for _, k := range page {
			if k.UpdatedAt.After(cursor) {
				cursor = k.UpdatedAt
			}
		}

		if len(page) < limit {
			break
		}
	}

	return all, cursor, nil
}
