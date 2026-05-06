package stewardclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/config"
)

// APIKeyRecord is the API key representation returned by Butler's key-sync endpoint.
type APIKeyRecord struct {
	ID                      uuid.UUID `json:"id"`
	KeyHash                 string    `json:"key_hash"`
	Name                    string    `json:"name"`
	Description             *string   `json:"description,omitempty"`
	IsActive                bool      `json:"is_active"`
	UserID                  *uuid.UUID `json:"user_id,omitempty"`
	OrgID                   *uuid.UUID `json:"org_id,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	RevokedAt               *time.Time `json:"revoked_at,omitempty"`
	UpdatedAt               time.Time  `json:"updated_at"`
	DeprecatedModelBehavior string     `json:"deprecated_model_behavior"`
}

// KeySyncStore is the minimal storage interface required by the key-sync worker.
type KeySyncStore interface {
	// UpsertAPIKeys inserts or updates API keys in the local database.
	UpsertAPIKeys(ctx context.Context, keys []APIKeyRecord) error
}

// KeySyncer polls Butler's key-sync endpoint and upserts API keys into the
// local database. It uses a cursor (updated_after) to fetch only changed keys.
type KeySyncer struct {
	cfg    config.StewardConfig
	store  KeySyncStore
	client *http.Client
	cursor time.Time
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewKeySyncer creates a KeySyncer. Call Start() to begin background polling.
func NewKeySyncer(cfg config.StewardConfig, store KeySyncStore) *KeySyncer {
	return &KeySyncer{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 15 * time.Second},
		// Start cursor at zero — first poll fetches all active keys.
		cursor: time.Time{},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background key-sync goroutine. It must be called once.
func (ks *KeySyncer) Start() {
	go ks.run()
}

// Stop signals the syncer to exit.
func (ks *KeySyncer) Stop() {
	close(ks.stopCh)
	<-ks.doneCh
}

func (ks *KeySyncer) run() {
	defer close(ks.doneCh)

	// Run immediately on startup so the steward has keys before it starts serving.
	ks.sync()

	ticker := time.NewTicker(ks.cfg.KeySyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ks.sync()
		case <-ks.stopCh:
			return
		}
	}
}

func (ks *KeySyncer) sync() {
	keys, newCursor, err := ks.fetch()
	if err != nil {
		slog.Warn("key sync fetch failed", "error", err)
		return
	}
	if len(keys) == 0 {
		return
	}

	if err := ks.store.UpsertAPIKeys(context.Background(), keys); err != nil {
		slog.Warn("key sync upsert failed", "error", err, "count", len(keys))
		return
	}

	ks.cursor = newCursor
	slog.Debug("key sync complete", "upserted", len(keys))
}

func (ks *KeySyncer) fetch() ([]APIKeyRecord, time.Time, error) {
	const limit = 200
	var all []APIKeyRecord
	cursor := ks.cursor

	for {
		url := fmt.Sprintf("%s/api/v1/steward/api-keys?updated_after=%s&limit=%d",
			ks.cfg.ButlerBaseURL,
			cursor.UTC().Format(time.RFC3339Nano),
			limit,
		)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			return nil, cursor, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+ks.cfg.StewardToken)

		resp, err := ks.client.Do(req)
		if err != nil {
			return nil, cursor, fmt.Errorf("get keys: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, cursor, fmt.Errorf("butler returned %d", resp.StatusCode)
		}

		var page []APIKeyRecord
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, cursor, fmt.Errorf("decode response: %w", err)
		}

		all = append(all, page...)

		if len(page) < limit {
			// Advance cursor to the latest updated_at in this batch.
			for _, k := range page {
				if k.UpdatedAt.After(cursor) {
					cursor = k.UpdatedAt
				}
			}
			break
		}

		// Advance cursor for the next page.
		for _, k := range page {
			if k.UpdatedAt.After(cursor) {
				cursor = k.UpdatedAt
			}
		}
	}

	return all, cursor, nil
}
