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

// ExperimentArmRecord is the arm representation received from Butler's experiment-sync endpoint.
type ExperimentArmRecord struct {
	ID           uuid.UUID `json:"id"`
	ExperimentID uuid.UUID `json:"experiment_id"`
	Name         string    `json:"name"`
	Model        string    `json:"model"`
	Weight       int       `json:"weight"`
	IsControl    bool      `json:"is_control"`
}

// ExperimentRecord is the experiment representation received from Butler's experiment-sync endpoint.
type ExperimentRecord struct {
	ID              uuid.UUID            `json:"id"`
	OrgID           uuid.UUID            `json:"org_id"`
	Status          string               `json:"status"`
	APIKeyID        *uuid.UUID           `json:"api_key_id,omitempty"`
	MetadataFilters map[string]string    `json:"metadata_filters"`
	StickyKey       *string              `json:"sticky_key,omitempty"`
	StartsAt        time.Time            `json:"starts_at"`
	EndsAt          time.Time            `json:"ends_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
	Arms            []ExperimentArmRecord `json:"arms"`
}

// ExperimentSyncStore is the minimal storage interface required by the experiment-sync worker.
type ExperimentSyncStore interface {
	// UpsertExperiments inserts or updates experiments and their arms in the local database.
	UpsertExperiments(ctx context.Context, records []ExperimentRecord) error
}

// ExperimentSyncer polls Butler's experiment-sync endpoint and upserts experiment
// definitions into the local database. It uses a cursor (updated_after) to fetch
// only changed experiments.
type ExperimentSyncer struct {
	cfg    config.StewardConfig
	store  ExperimentSyncStore
	client *http.Client
	cursor time.Time
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewExperimentSyncer creates an ExperimentSyncer. Call Start() to begin background polling.
func NewExperimentSyncer(cfg config.StewardConfig, store ExperimentSyncStore) *ExperimentSyncer {
	return &ExperimentSyncer{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 15 * time.Second},
		// Zero cursor — first poll fetches all non-draft experiments.
		cursor: time.Time{},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background experiment-sync goroutine. It must be called once.
func (es *ExperimentSyncer) Start() {
	go es.run()
}

// Stop signals the syncer to exit and waits for it to finish.
func (es *ExperimentSyncer) Stop() {
	close(es.stopCh)
	<-es.doneCh
}

func (es *ExperimentSyncer) run() {
	defer close(es.doneCh)

	// Sync immediately on startup so the steward has experiment data before serving.
	es.sync()

	ticker := time.NewTicker(es.cfg.KeySyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			es.sync()
		case <-es.stopCh:
			return
		}
	}
}

func (es *ExperimentSyncer) sync() {
	records, newCursor, err := es.fetch()
	if err != nil {
		slog.Warn("experiment sync fetch failed", "error", err)
		return
	}
	if len(records) == 0 {
		return
	}

	if err := es.store.UpsertExperiments(context.Background(), records); err != nil {
		slog.Warn("experiment sync upsert failed", "error", err, "count", len(records))
		return
	}

	es.cursor = newCursor
	slog.Debug("experiment sync complete", "upserted", len(records))
}

func (es *ExperimentSyncer) fetch() ([]ExperimentRecord, time.Time, error) {
	const limit = 200
	var all []ExperimentRecord
	cursor := es.cursor

	for {
		url := fmt.Sprintf("%s/api/v1/steward/experiments?updated_after=%s&limit=%d",
			es.cfg.ButlerBaseURL,
			cursor.UTC().Format(time.RFC3339Nano),
			limit,
		)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			return nil, cursor, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+es.cfg.StewardToken)

		resp, err := es.client.Do(req)
		if err != nil {
			return nil, cursor, fmt.Errorf("get experiments: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, cursor, fmt.Errorf("butler returned %d", resp.StatusCode)
		}

		var page []ExperimentRecord
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, cursor, fmt.Errorf("decode response: %w", err)
		}

		all = append(all, page...)

		if len(page) < limit {
			// Advance cursor to the latest updated_at in this batch.
			for _, e := range page {
				if e.UpdatedAt.After(cursor) {
					cursor = e.UpdatedAt
				}
			}
			break
		}

		// Advance cursor for the next page.
		for _, e := range page {
			if e.UpdatedAt.After(cursor) {
				cursor = e.UpdatedAt
			}
		}
	}

	return all, cursor, nil
}
