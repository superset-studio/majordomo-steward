package stewardclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/models"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
)

// providerKeyPayload mirrors Butler's ingest.ProviderKeyRecord wire format.
// Key is plaintext on the wire; this Steward re-encrypts before persisting.
type providerKeyPayload struct {
	ID        uuid.UUID  `json:"id"`
	UserID    *uuid.UUID `json:"user_id,omitempty"`
	OrgID     *uuid.UUID `json:"org_id,omitempty"`
	Provider  string     `json:"provider"`
	Key       string     `json:"key"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ProviderKeySyncStore is the minimal storage interface required by the
// provider key syncer. UpsertBatch persists already-locally-encrypted keys.
type ProviderKeySyncStore interface {
	UpsertBatch(ctx context.Context, keys []*models.ProviderAPIKey) error
}

// ProviderKeySyncer periodically polls Butler for all provider API keys
// scoped to the steward's org, re-encrypts them with the Steward's local
// secret store, and upserts into the local provider_api_keys table.
type ProviderKeySyncer struct {
	cfg         config.StewardConfig
	store       ProviderKeySyncStore
	secretStore secrets.SecretStore
	client      *http.Client
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// NewProviderKeySyncer creates a ProviderKeySyncer. Call Start() to begin polling.
func NewProviderKeySyncer(cfg config.StewardConfig, store ProviderKeySyncStore, secretStore secrets.SecretStore) *ProviderKeySyncer {
	return &ProviderKeySyncer{
		cfg:         cfg,
		store:       store,
		secretStore: secretStore,
		client:      &http.Client{Timeout: 30 * time.Second},
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Start launches the background sync goroutine. It must be called once.
func (ps *ProviderKeySyncer) Start() {
	go ps.run()
}

// Stop signals the syncer to exit and waits for it to finish.
func (ps *ProviderKeySyncer) Stop() {
	close(ps.stopCh)
	<-ps.doneCh
}

func (ps *ProviderKeySyncer) run() {
	defer close(ps.doneCh)

	// Sync immediately on startup so provider keys are available before the
	// local worker tries to claim any runs.
	ps.sync()

	ticker := time.NewTicker(ps.cfg.KeySyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ps.sync()
		case <-ps.stopCh:
			return
		}
	}
}

func (ps *ProviderKeySyncer) sync() {
	payloads, err := ps.fetch()
	if err != nil {
		slog.Warn("provider key sync fetch failed", "error", err)
		return
	}

	records, err := ps.encryptAndConvert(payloads)
	if err != nil {
		slog.Warn("provider key sync encrypt failed", "error", err)
		return
	}

	if len(records) == 0 {
		return
	}

	if err := ps.store.UpsertBatch(context.Background(), records); err != nil {
		slog.Warn("provider key sync upsert failed", "error", err, "count", len(records))
		return
	}

	slog.Debug("provider key sync complete", "upserted", len(records))
}

func (ps *ProviderKeySyncer) fetch() ([]providerKeyPayload, error) {
	url := ps.cfg.ButlerBaseURL + "/api/v1/steward/provider-keys"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, bytes.NewReader(nil))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+ps.cfg.StewardToken)

	resp, err := ps.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get provider keys: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var payloads []providerKeyPayload
	if err := json.NewDecoder(resp.Body).Decode(&payloads); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payloads, nil
}

// encryptAndConvert re-encrypts plaintext keys from Butler using the
// Steward's own secret store before persisting locally.
func (ps *ProviderKeySyncer) encryptAndConvert(payloads []providerKeyPayload) ([]*models.ProviderAPIKey, error) {
	records := make([]*models.ProviderAPIKey, 0, len(payloads))
	for _, p := range payloads {
		if p.Key == "" {
			slog.Warn("provider key sync: skipping record with empty key",
				"key_id", p.ID, "provider", p.Provider)
			continue
		}
		enc, err := ps.secretStore.Encrypt(p.Key)
		if err != nil {
			return nil, fmt.Errorf("encrypt provider key %s: %w", p.ID, err)
		}
		records = append(records, &models.ProviderAPIKey{
			ID:           p.ID,
			UserID:       p.UserID,
			OrgID:        p.OrgID,
			Provider:     p.Provider,
			EncryptedKey: enc,
			CreatedAt:    p.CreatedAt,
			UpdatedAt:    p.UpdatedAt,
		})
	}
	return records, nil
}
