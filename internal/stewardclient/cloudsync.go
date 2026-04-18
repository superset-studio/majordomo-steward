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

// cloudStorageConfigPayload mirrors ingest.CloudStorageConfigRecord from Butler.
// Defined here to avoid a cross-repo import dependency.
type cloudStorageConfigPayload struct {
	OwnerID   uuid.UUID `json:"owner_id"`
	OwnerType string    `json:"owner_type"`
	Provider  string    `json:"provider"`
	// S3 — plaintext from Butler, re-encrypted before local storage
	S3Bucket          *string `json:"s3_bucket,omitempty"`
	S3Region          *string `json:"s3_region,omitempty"`
	S3Endpoint        *string `json:"s3_endpoint,omitempty"`
	S3AccessKeyID     *string `json:"s3_access_key_id,omitempty"`
	S3SecretAccessKey *string `json:"s3_secret_access_key,omitempty"`
	// GCS — plaintext from Butler, re-encrypted before local storage
	GCSBucket          *string `json:"gcs_bucket,omitempty"`
	GCSProjectID       *string `json:"gcs_project_id,omitempty"`
	GCSCredentialsJSON *string `json:"gcs_credentials_json,omitempty"`
}

// CloudStorageSyncStore is the minimal storage interface required by
// CloudStorageSyncer.
type CloudStorageSyncStore interface {
	ReplaceCloudStorageConfigs(ctx context.Context, records []models.CloudStorageRecord) error
}

// CloudStorageSyncer polls Butler for the org's cloud storage configs and
// stores them locally so the proxy handler can look them up without calling
// back to Butler on the hot path.
type CloudStorageSyncer struct {
	cfg         config.StewardConfig
	store       CloudStorageSyncStore
	secretStore secrets.SecretStore
	client      *http.Client
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// NewCloudStorageSyncer creates a CloudStorageSyncer. Call Start() to begin
// background syncing.
func NewCloudStorageSyncer(
	cfg config.StewardConfig,
	store CloudStorageSyncStore,
	secretStore secrets.SecretStore,
) *CloudStorageSyncer {
	return &CloudStorageSyncer{
		cfg:         cfg,
		store:       store,
		secretStore: secretStore,
		client:      &http.Client{Timeout: 15 * time.Second},
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Start launches the background sync goroutine. Call Stop to shut it down.
func (cs *CloudStorageSyncer) Start() {
	go cs.run()
}

// Stop signals the syncer to exit and blocks until it does.
func (cs *CloudStorageSyncer) Stop() {
	close(cs.stopCh)
	<-cs.doneCh
}

func (cs *CloudStorageSyncer) run() {
	defer close(cs.doneCh)

	// Sync immediately on startup so configs are available before requests arrive.
	cs.sync()

	ticker := time.NewTicker(cs.cfg.KeySyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs.sync()
		case <-cs.stopCh:
			return
		}
	}
}

func (cs *CloudStorageSyncer) sync() {
	payloads, err := cs.fetch()
	if err != nil {
		slog.Warn("cloud storage sync fetch failed", "error", err)
		return
	}

	records, err := cs.encryptAndConvert(payloads)
	if err != nil {
		slog.Warn("cloud storage sync encrypt failed", "error", err)
		return
	}

	if err := cs.store.ReplaceCloudStorageConfigs(context.Background(), records); err != nil {
		slog.Warn("cloud storage sync store failed", "error", err)
		return
	}

	slog.Debug("cloud storage sync complete", "configs", len(records))
}

func (cs *CloudStorageSyncer) fetch() ([]cloudStorageConfigPayload, error) {
	url := cs.cfg.ButlerBaseURL + "/api/v1/steward/cloud-storage-configs"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, bytes.NewReader(nil))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cs.cfg.StewardToken)

	resp, err := cs.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get cloud storage configs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var payloads []cloudStorageConfigPayload
	if err := json.NewDecoder(resp.Body).Decode(&payloads); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return payloads, nil
}

// encryptAndConvert re-encrypts plaintext credentials from Butler using the
// Steward's own secret store before persisting locally.
func (cs *CloudStorageSyncer) encryptAndConvert(payloads []cloudStorageConfigPayload) ([]models.CloudStorageRecord, error) {
	records := make([]models.CloudStorageRecord, 0, len(payloads))

	for _, p := range payloads {
		r := models.CloudStorageRecord{
			OwnerID:   p.OwnerID,
			OwnerType: p.OwnerType,
			Provider:  p.Provider,
			S3Bucket:  p.S3Bucket,
			S3Region:  p.S3Region,
			S3Endpoint: p.S3Endpoint,
			GCSBucket:  p.GCSBucket,
			GCSProjectID: p.GCSProjectID,
		}

		switch models.CloudStorageProviderType(p.Provider) {
		case models.CloudStorageProviderGCS:
			if p.GCSCredentialsJSON == nil {
				slog.Warn("cloud storage sync: skipping GCS config with missing credentials",
					"owner_id", p.OwnerID)
				continue
			}
			enc, err := cs.secretStore.Encrypt(*p.GCSCredentialsJSON)
			if err != nil {
				return nil, fmt.Errorf("encrypt GCS credentials for owner %s: %w", p.OwnerID, err)
			}
			r.GCSCredentialsJSONEncrypted = &enc

		default: // s3
			if p.S3AccessKeyID == nil || p.S3SecretAccessKey == nil {
				slog.Warn("cloud storage sync: skipping S3 config with missing credentials",
					"owner_id", p.OwnerID)
				continue
			}
			encKey, err := cs.secretStore.Encrypt(*p.S3AccessKeyID)
			if err != nil {
				return nil, fmt.Errorf("encrypt S3 access key ID for owner %s: %w", p.OwnerID, err)
			}
			encSecret, err := cs.secretStore.Encrypt(*p.S3SecretAccessKey)
			if err != nil {
				return nil, fmt.Errorf("encrypt S3 secret key for owner %s: %w", p.OwnerID, err)
			}
			r.S3AccessKeyIDEncrypted = &encKey
			r.S3SecretAccessKeyEncrypted = &encSecret
		}

		records = append(records, r)
	}

	return records, nil
}
