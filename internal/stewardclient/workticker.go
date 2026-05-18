package stewardclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/config"
)

// Sync job type identifiers — must match the steward_jobs.job_type CHECK on butler.
const (
	JobTypeSyncAPIKeys      = "sync_api_keys"
	JobTypeSyncCloudStorage = "sync_cloud_storage"
	JobTypeSyncProviderKeys = "sync_provider_keys"
	JobTypeSyncExperiments  = "sync_experiments"
	JobTypeReplay           = "replay"
	JobTypeEval             = "eval"
)

// WorkExecutor runs replay/eval jobs received via the work tick.
// Satisfied by the steward package's jobExecutor.
type WorkExecutor interface {
	ExecuteReplay(ctx context.Context, orgID, runID uuid.UUID) error
	ExecuteEval(ctx context.Context, orgID, runID uuid.UUID) error
}

// workItem is the wire representation of one entry returned by GET /work.
type workItem struct {
	JobID   uuid.UUID  `json:"job_id"`
	OrgID   uuid.UUID  `json:"org_id"`
	JobType string     `json:"job_type"`
	RunID   *uuid.UUID `json:"run_id,omitempty"`
}

// WorkTicker is the single per-org poller that replaces all per-entity sync
// workers and the old JobPoller. Each tick:
//
//  1. GET /api/v1/steward/work — Butler atomically claims a batch of pending
//     jobs (sync_* and/or replay/eval) and returns them.
//  2. For each item, dispatch to the matching syncer or executor.
//  3. ack each successful item, fail each errored item — independently, so
//     partial-tick failures don't block other work.
type WorkTicker struct {
	cfg            config.StewardConfig
	client         *http.Client
	keySync        *KeySyncer
	cloudSync      *CloudStorageSyncer
	providerSync   *ProviderKeySyncer
	experimentSync *ExperimentSyncer
	executor       WorkExecutor

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewWorkTicker constructs a WorkTicker. Syncers may be nil if the
// corresponding capability is not configured for this steward (e.g.
// cloudSync/providerSync are nil when the secret store is not set).
func NewWorkTicker(
	cfg config.StewardConfig,
	keySync *KeySyncer,
	cloudSync *CloudStorageSyncer,
	providerSync *ProviderKeySyncer,
	experimentSync *ExperimentSyncer,
	executor WorkExecutor,
) *WorkTicker {
	return &WorkTicker{
		cfg:            cfg,
		client:         &http.Client{Timeout: 30 * time.Second},
		keySync:        keySync,
		cloudSync:      cloudSync,
		providerSync:   providerSync,
		experimentSync: experimentSync,
		executor:       executor,
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

// Start launches the polling goroutine. Call once.
func (wt *WorkTicker) Start() {
	go wt.run()
}

// Stop signals the ticker to exit and waits for it.
func (wt *WorkTicker) Stop() {
	close(wt.stopCh)
	<-wt.doneCh
}

func (wt *WorkTicker) run() {
	defer close(wt.doneCh)

	// Spread concurrent org tickers across the interval to dampen herd effects.
	jitter := time.Duration(rand.Int63n(int64(wt.cfg.WorkTickInterval)))
	select {
	case <-time.After(jitter):
	case <-wt.stopCh:
		return
	}

	// First tick immediately so a freshly-started steward fetches state before
	// serving requests.
	wt.tick()

	ticker := time.NewTicker(wt.cfg.WorkTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wt.tick()
		case <-wt.stopCh:
			return
		}
	}
}

func (wt *WorkTicker) tick() {
	tickID := uuid.NewString()
	tickStart := time.Now()
	logger := slog.With(
		"tick_id", tickID,
		"org_id", wt.cfg.OrgID,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	items, err := wt.fetchWork(ctx)
	if err != nil {
		logger.Warn("work tick: fetch failed", "error", err)
		return
	}

	if len(items) == 0 {
		logger.Info("work tick: no work", "duration_ms", time.Since(tickStart).Milliseconds())
		return
	}

	var succeeded, failed int
	for _, item := range items {
		itemLogger := logger.With(
			"job_id", item.JobID,
			"job_type", item.JobType,
		)
		itemStart := time.Now()

		if err := wt.dispatch(ctx, itemLogger, item); err != nil {
			itemLogger.Warn("work item failed",
				"error", err,
				"duration_ms", time.Since(itemStart).Milliseconds(),
			)
			wt.markResult(ctx, itemLogger, item.JobID, false)
			failed++
			continue
		}

		itemLogger.Info("work item succeeded",
			"duration_ms", time.Since(itemStart).Milliseconds(),
		)
		wt.markResult(ctx, itemLogger, item.JobID, true)
		succeeded++
	}

	logger.Info("work tick complete",
		"items", len(items),
		"succeeded", succeeded,
		"failed", failed,
		"duration_ms", time.Since(tickStart).Milliseconds(),
	)
}

func (wt *WorkTicker) dispatch(ctx context.Context, logger *slog.Logger, item workItem) error {
	switch item.JobType {
	case JobTypeSyncAPIKeys:
		if wt.keySync == nil {
			return fmt.Errorf("sync_api_keys received but keySync is not configured")
		}
		return wt.keySync.Sync(ctx, logger)

	case JobTypeSyncCloudStorage:
		if wt.cloudSync == nil {
			return fmt.Errorf("sync_cloud_storage received but cloudSync is not configured")
		}
		return wt.cloudSync.Sync(ctx, logger)

	case JobTypeSyncProviderKeys:
		if wt.providerSync == nil {
			return fmt.Errorf("sync_provider_keys received but providerSync is not configured")
		}
		return wt.providerSync.Sync(ctx, logger)

	case JobTypeSyncExperiments:
		if wt.experimentSync == nil {
			return fmt.Errorf("sync_experiments received but experimentSync is not configured")
		}
		return wt.experimentSync.Sync(ctx, logger)

	case JobTypeReplay:
		if item.RunID == nil {
			return fmt.Errorf("replay job missing run_id")
		}
		return wt.executor.ExecuteReplay(ctx, item.OrgID, *item.RunID)

	case JobTypeEval:
		if item.RunID == nil {
			return fmt.Errorf("eval job missing run_id")
		}
		return wt.executor.ExecuteEval(ctx, item.OrgID, *item.RunID)

	default:
		return fmt.Errorf("unknown job_type %q", item.JobType)
	}
}

func (wt *WorkTicker) fetchWork(ctx context.Context) ([]workItem, error) {
	url := fmt.Sprintf("%s/api/v1/steward/work?limit=%d",
		wt.cfg.ButlerBaseURL, wt.cfg.WorkTickLimit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+wt.cfg.StewardToken)

	resp, err := wt.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get work: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var items []workItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return items, nil
}

func (wt *WorkTicker) markResult(ctx context.Context, logger *slog.Logger, jobID uuid.UUID, success bool) {
	suffix := "fail"
	if success {
		suffix = "ack"
	}
	url := fmt.Sprintf("%s/api/v1/steward/work/%s/%s",
		wt.cfg.ButlerBaseURL, jobID, suffix)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(nil))
	if err != nil {
		logger.Warn("work result: build request failed", "error", err, "result", suffix)
		return
	}
	req.Header.Set("Authorization", "Bearer "+wt.cfg.StewardToken)

	resp, err := wt.client.Do(req)
	if err != nil {
		logger.Warn("work result: post failed", "error", err, "result", suffix)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Warn("work result: butler returned non-OK", "status", resp.StatusCode, "result", suffix)
	}
}
