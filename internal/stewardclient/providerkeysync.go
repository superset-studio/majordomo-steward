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
	"github.com/superset-studio/majordomo-steward/internal/models"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
)

// providerKeyPayload mirrors Butler's ingest.ProviderKeyRecord wire format.
// Key is plaintext on the wire; Steward re-encrypts before persisting.
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
// provider key syncer.
type ProviderKeySyncStore interface {
	UpsertBatch(ctx context.Context, keys []*models.ProviderAPIKey) error
}

// ProviderKeySyncer fetches all provider API keys for the steward's org from
// Butler, re-encrypts them, and upserts them locally. Invoked by WorkTicker
// when a sync_provider_keys job is received.
type ProviderKeySyncer struct {
	cfg         config.StewardConfig
	store       ProviderKeySyncStore
	secretStore secrets.SecretStore
	client      *http.Client
}

// NewProviderKeySyncer constructs a ProviderKeySyncer.
func NewProviderKeySyncer(cfg config.StewardConfig, store ProviderKeySyncStore, secretStore secrets.SecretStore) *ProviderKeySyncer {
	return &ProviderKeySyncer{
		cfg:         cfg,
		store:       store,
		secretStore: secretStore,
		client:      &http.Client{Timeout: 30 * time.Second},
	}
}

// Sync fetches provider keys from Butler and upserts them locally. The passed
// logger should already carry tick_id / org_id / job_id context.
func (ps *ProviderKeySyncer) Sync(ctx context.Context, logger *slog.Logger) error {
	payloads, err := ps.fetch(ctx)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}

	records, err := ps.encryptAndConvert(logger, payloads)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	if len(records) == 0 {
		logger.Info("provider key sync: no records")
		return nil
	}

	if err := ps.store.UpsertBatch(ctx, records); err != nil {
		return fmt.Errorf("upsert %d keys: %w", len(records), err)
	}

	logger.Info("provider key sync applied", "upserted", len(records))
	return nil
}

func (ps *ProviderKeySyncer) fetch(ctx context.Context) ([]providerKeyPayload, error) {
	url := ps.cfg.ButlerBaseURL + "/api/v1/steward/provider-keys"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

func (ps *ProviderKeySyncer) encryptAndConvert(logger *slog.Logger, payloads []providerKeyPayload) ([]*models.ProviderAPIKey, error) {
	records := make([]*models.ProviderAPIKey, 0, len(payloads))
	for _, p := range payloads {
		if p.Key == "" {
			logger.Warn("provider key sync: skipping record with empty key",
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
