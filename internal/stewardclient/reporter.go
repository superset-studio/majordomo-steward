// Package stewardclient provides the HTTP client workers that the Steward uses
// to communicate with the Butler: metadata reporting, API key sync, and job polling.
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
)

// MetadataRecord mirrors the ingest.MetadataRecord type in Butler.
// Defined here to avoid a cross-repo import dependency.
type MetadataRecord struct {
	ID                  uuid.UUID              `json:"id"`
	UserID              *uuid.UUID             `json:"user_id,omitempty"`
	MajordomoAPIKeyID   *uuid.UUID             `json:"majordomo_api_key_id,omitempty"`
	ProxyKeyID          *uuid.UUID             `json:"proxy_key_id,omitempty"`
	ProviderAPIKeyHash  string                 `json:"provider_api_key_hash,omitempty"`
	ProviderAPIKeyAlias string                 `json:"provider_api_key_alias,omitempty"`
	Provider            string                 `json:"provider"`
	Model               string                 `json:"model"`
	RequestPath         string                 `json:"request_path"`
	RequestMethod       string                 `json:"request_method"`
	RequestedAt         time.Time              `json:"requested_at"`
	RespondedAt         time.Time              `json:"responded_at"`
	ResponseTimeMS      int                    `json:"response_time_ms"`
	InputTokens         int                    `json:"input_tokens"`
	OutputTokens        int                    `json:"output_tokens"`
	CachedTokens        int                    `json:"cached_tokens"`
	CacheCreationTokens int                    `json:"cache_creation_tokens"`
	InputCost           float64                `json:"input_cost"`
	OutputCost          float64                `json:"output_cost"`
	TotalCost           float64                `json:"total_cost"`
	StatusCode          int                    `json:"status_code"`
	ErrorMessage        *string                `json:"error_message,omitempty"`
	RawMetadata         map[string]interface{} `json:"raw_metadata,omitempty"`
	IndexedMetadata     map[string]interface{} `json:"indexed_metadata,omitempty"`
	BodyS3Key           *string                `json:"body_s3_key,omitempty"`
	ModelAliasFound     bool                   `json:"model_alias_found"`
	OrgID               *uuid.UUID             `json:"org_id,omitempty"`
	ExperimentID        *uuid.UUID             `json:"experiment_id,omitempty"`
	ExperimentArmID     *uuid.UUID             `json:"experiment_arm_id,omitempty"`
	OriginalModel       *string                `json:"original_model,omitempty"`
}

// ReporterStore is the minimal storage interface required by the Reporter.
type ReporterStore interface {
	// FetchUnsyncedRecords returns up to limit records not yet synced to Butler
	// for the given org, ordered by created_at ascending.
	FetchUnsyncedRecords(ctx context.Context, orgID uuid.UUID, limit int) ([]MetadataRecord, error)
	// MarkSynced marks the given request IDs as synced to Butler.
	MarkSynced(ctx context.Context, ids []uuid.UUID) error
}

// Reporter polls the local DB for unsynced request logs and batches them to Butler.
// Using the DB as the queue makes the reporter crash-safe — records persisted by
// the proxy write loop will not be lost if the reporter restarts.
type Reporter struct {
	cfg    config.StewardConfig
	store  ReporterStore
	client *http.Client
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewReporter creates a Reporter. Call Start() to begin background flushing.
func NewReporter(cfg config.StewardConfig, store ReporterStore) *Reporter {
	return &Reporter{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 30 * time.Second},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background flush goroutine.
func (r *Reporter) Start() {
	go r.run()
}

// Stop signals the reporter to do a final flush and exit. It blocks until done.
func (r *Reporter) Stop() {
	close(r.stopCh)
	<-r.doneCh
}

func (r *Reporter) run() {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.cfg.BatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.flush()
		case <-r.stopCh:
			r.flush() // drain on shutdown
			return
		}
	}
}

func (r *Reporter) flush() {
	ctx := context.Background()

	for {
		records, err := r.store.FetchUnsyncedRecords(ctx, r.cfg.OrgID, r.cfg.BatchMaxSize)
		if err != nil {
			slog.Warn("reporter: fetch unsynced records failed", "error", err)
			return
		}
		if len(records) == 0 {
			return
		}

		ids := make([]uuid.UUID, len(records))
		for i, rec := range records {
			ids[i] = rec.ID
		}

		if err := r.send(records); err != nil {
			slog.Warn("reporter: batch send failed", "error", err, "records", len(records))
			return
		}

		if err := r.store.MarkSynced(ctx, ids); err != nil {
			slog.Warn("reporter: mark synced failed", "error", err, "records", len(records))
			return
		}

		slog.Debug("reporter: batch sent", "records", len(records))

		// If we got a full page, there may be more — loop immediately.
		if len(records) < r.cfg.BatchMaxSize {
			return
		}
	}
}

func (r *Reporter) send(records []MetadataRecord) error {
	payload := map[string]interface{}{"records": records}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	url := r.cfg.ButlerBaseURL + "/api/v1/steward/ingest/metadata"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.cfg.StewardToken)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("post batch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	return nil
}
