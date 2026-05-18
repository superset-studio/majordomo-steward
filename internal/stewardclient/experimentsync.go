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

// ExperimentArmRecord is the arm representation returned by Butler's experiment-sync endpoint.
type ExperimentArmRecord struct {
	ID           uuid.UUID `json:"id"`
	ExperimentID uuid.UUID `json:"experiment_id"`
	Name         string    `json:"name"`
	Model        string    `json:"model"`
	Weight       int       `json:"weight"`
	IsControl    bool      `json:"is_control"`
}

// ExperimentRecord is the experiment representation returned by Butler's experiment-sync endpoint.
type ExperimentRecord struct {
	ID              uuid.UUID             `json:"id"`
	OrgID           uuid.UUID             `json:"org_id"`
	Status          string                `json:"status"`
	APIKeyID        *uuid.UUID            `json:"api_key_id,omitempty"`
	MetadataFilters map[string]string     `json:"metadata_filters"`
	StickyKey       *string               `json:"sticky_key,omitempty"`
	StartsAt        time.Time             `json:"starts_at"`
	EndsAt          time.Time             `json:"ends_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	Arms            []ExperimentArmRecord `json:"arms"`
}

// ExperimentSyncStore is the minimal storage interface required by the experiment syncer.
type ExperimentSyncStore interface {
	UpsertExperiments(ctx context.Context, records []ExperimentRecord) error
}

// ExperimentSyncer fetches changed experiments from Butler and upserts them
// locally. Cursor-based (updated_after watermark). Invoked by WorkTicker when
// a sync_experiments job is received.
type ExperimentSyncer struct {
	cfg    config.StewardConfig
	store  ExperimentSyncStore
	client *http.Client

	mu     sync.Mutex
	cursor time.Time
}

// NewExperimentSyncer constructs an ExperimentSyncer with a zero cursor
// (first sync fetches all non-draft experiments).
func NewExperimentSyncer(cfg config.StewardConfig, store ExperimentSyncStore) *ExperimentSyncer {
	return &ExperimentSyncer{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// Sync fetches all experiments changed since the last cursor and upserts them.
// The cursor advances only on a successful upsert. The passed logger should
// already carry tick_id / org_id / job_id context.
func (es *ExperimentSyncer) Sync(ctx context.Context, logger *slog.Logger) error {
	es.mu.Lock()
	cursor := es.cursor
	es.mu.Unlock()

	records, newCursor, err := es.fetch(ctx, cursor)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(records) == 0 {
		logger.Info("experiment sync: no changes", "cursor", cursor)
		return nil
	}

	if err := es.store.UpsertExperiments(ctx, records); err != nil {
		return fmt.Errorf("upsert %d experiments: %w", len(records), err)
	}

	es.mu.Lock()
	es.cursor = newCursor
	es.mu.Unlock()

	logger.Info("experiment sync applied", "upserted", len(records), "new_cursor", newCursor)
	return nil
}

func (es *ExperimentSyncer) fetch(ctx context.Context, cursor time.Time) ([]ExperimentRecord, time.Time, error) {
	const limit = 200
	var all []ExperimentRecord

	for {
		url := fmt.Sprintf("%s/api/v1/steward/experiments?updated_after=%s&limit=%d",
			es.cfg.ButlerBaseURL,
			cursor.UTC().Format(time.RFC3339Nano),
			limit,
		)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, cursor, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+es.cfg.StewardToken)

		resp, err := es.client.Do(req)
		if err != nil {
			return nil, cursor, fmt.Errorf("get experiments: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, cursor, fmt.Errorf("butler returned %d", resp.StatusCode)
		}

		var page []ExperimentRecord
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, cursor, fmt.Errorf("decode response: %w", err)
		}
		resp.Body.Close()

		all = append(all, page...)
		for _, e := range page {
			if e.UpdatedAt.After(cursor) {
				cursor = e.UpdatedAt
			}
		}

		if len(page) < limit {
			break
		}
	}

	return all, cursor, nil
}
