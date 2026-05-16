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
)

// ResultsReporterStore is the minimal storage interface required by the
// ResultsReporter. Each Fetch returns up to limit unsynced rows, and each Mark
// stamps the synced_at column on the given ids.
type ResultsReporterStore interface {
	FetchUnsyncedReplayRuns(ctx context.Context, limit int) ([]*models.ReplayRun, error)
	MarkReplayRunsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedReplayResults(ctx context.Context, limit int) ([]*models.ReplayResult, error)
	MarkReplayResultsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedEvalRuns(ctx context.Context, limit int) ([]*models.EvalRun, error)
	MarkEvalRunsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedEvalResults(ctx context.Context, limit int) ([]*models.EvalResult, error)
	MarkEvalResultsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedEvalResultScores(ctx context.Context, limit int) ([]*models.EvalResultScore, error)
	MarkEvalResultScoresSynced(ctx context.Context, ids []uuid.UUID) error
}

// ResultsReporter polls the local DB for unsynced replay and eval run status,
// results, and scores, batches them, POSTs to Butler, and marks rows synced.
// Using the DB as the queue makes the reporter crash-safe — partial flushes
// resume after a restart from wherever synced_at left off.
type ResultsReporter struct {
	cfg    config.StewardConfig
	store  ResultsReporterStore
	client *http.Client
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewResultsReporter creates a ResultsReporter. Call Start() to begin polling.
func NewResultsReporter(cfg config.StewardConfig, store ResultsReporterStore) *ResultsReporter {
	return &ResultsReporter{
		cfg:    cfg,
		store:  store,
		client: &http.Client{Timeout: 60 * time.Second},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start launches the background reporter goroutine. It must be called once.
func (rr *ResultsReporter) Start() {
	go rr.run()
}

// Stop signals the reporter to exit and waits for it to finish.
func (rr *ResultsReporter) Stop() {
	close(rr.stopCh)
	<-rr.doneCh
}

func (rr *ResultsReporter) run() {
	defer close(rr.doneCh)

	ticker := time.NewTicker(rr.cfg.BatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rr.flushAll()
		case <-rr.stopCh:
			rr.flushAll()
			return
		}
	}
}

// flushAll drains each unsynced category once.
func (rr *ResultsReporter) flushAll() {
	rr.flushReplayRuns()
	rr.flushReplayResults()
	rr.flushEvalRuns()
	rr.flushEvalResults()
}

// reporterBatchSize caps a single sync iteration. Independent of the existing
// Reporter's BatchMaxSize since result rows are larger than metadata rows.
const reporterBatchSize = 500

func (rr *ResultsReporter) flushReplayRuns() {
	ctx := context.Background()
	runs, err := rr.store.FetchUnsyncedReplayRuns(ctx, reporterBatchSize)
	if err != nil {
		slog.Warn("results reporter: fetch replay runs failed", "error", err)
		return
	}
	if len(runs) == 0 {
		return
	}

	updates := make([]replayRunStatusUpdate, len(runs))
	ids := make([]uuid.UUID, len(runs))
	for i, run := range runs {
		updates[i] = replayRunStatusUpdate{
			ID:                   run.ID,
			Status:               run.Status,
			ErrorMessage:         run.ErrorMessage,
			StartedAt:            run.StartedAt,
			CompletedAt:          run.CompletedAt,
			TotalRequests:        run.TotalRequests,
			ExactMatches:         run.ExactMatches,
			JudgeEquivalent:      run.JudgeEquivalent,
			Divergent:            run.Divergent,
			OriginalTotalCost:    run.OriginalTotalCost,
			ReplayTotalCost:      run.ReplayTotalCost,
			OriginalAvgLatencyMs: run.OriginalAvgLatencyMs,
			ReplayAvgLatencyMs:   run.ReplayAvgLatencyMs,
		}
		ids[i] = run.ID
	}

	if err := rr.post("/api/v1/steward/replay-runs/status", updates); err != nil {
		slog.Warn("results reporter: post replay run statuses failed", "count", len(updates), "error", err)
		return
	}
	if err := rr.store.MarkReplayRunsSynced(ctx, ids); err != nil {
		slog.Warn("results reporter: mark replay runs synced failed", "count", len(ids), "error", err)
	}
}

func (rr *ResultsReporter) flushReplayResults() {
	ctx := context.Background()
	results, err := rr.store.FetchUnsyncedReplayResults(ctx, reporterBatchSize)
	if err != nil {
		slog.Warn("results reporter: fetch replay results failed", "error", err)
		return
	}
	if len(results) == 0 {
		return
	}

	payloads := make([]replayResultPayload, len(results))
	ids := make([]uuid.UUID, len(results))
	for i, res := range results {
		payloads[i] = replayResultPayload{
			ID:                   res.ID,
			ReplayRunID:          res.ReplayRunID,
			SourceRequestID:      res.SourceRequestID,
			OriginalProvider:     res.OriginalProvider,
			OriginalModel:        res.OriginalModel,
			OriginalCost:         res.OriginalCost,
			OriginalLatencyMs:    res.OriginalLatencyMs,
			OriginalInputTokens:  res.OriginalInputTokens,
			OriginalOutputTokens: res.OriginalOutputTokens,
			ReplayResponse:       res.ReplayResponse,
			ReplayCost:           res.ReplayCost,
			ReplayLatencyMs:      res.ReplayLatencyMs,
			ReplayInputTokens:    res.ReplayInputTokens,
			ReplayOutputTokens:   res.ReplayOutputTokens,
			ExactMatch:           res.ExactMatch,
			JudgeEquivalent:      res.JudgeEquivalent,
			JudgeReason:          res.JudgeReason,
			ErrorMessage:         res.ErrorMessage,
			CreatedAt:            res.CreatedAt,
		}
		ids[i] = res.ID
	}

	if err := rr.post("/api/v1/steward/replay-results", payloads); err != nil {
		slog.Warn("results reporter: post replay results failed", "count", len(payloads), "error", err)
		return
	}
	if err := rr.store.MarkReplayResultsSynced(ctx, ids); err != nil {
		slog.Warn("results reporter: mark replay results synced failed", "count", len(ids), "error", err)
	}
}

func (rr *ResultsReporter) flushEvalRuns() {
	ctx := context.Background()
	runs, err := rr.store.FetchUnsyncedEvalRuns(ctx, reporterBatchSize)
	if err != nil {
		slog.Warn("results reporter: fetch eval runs failed", "error", err)
		return
	}
	if len(runs) == 0 {
		return
	}

	updates := make([]evalRunStatusUpdate, len(runs))
	ids := make([]uuid.UUID, len(runs))
	for i, run := range runs {
		updates[i] = evalRunStatusUpdate{
			ID:                   run.ID,
			Status:               run.Status,
			ErrorMessage:         run.ErrorMessage,
			StartedAt:            run.StartedAt,
			CompletedAt:          run.CompletedAt,
			TotalRequests:        run.TotalRequests,
			SuccessfulRequests:   run.SuccessfulRequests,
			FailedRequests:       run.FailedRequests,
			OriginalTotalCost:    run.OriginalTotalCost,
			ReplayTotalCost:      run.ReplayTotalCost,
			JudgeTotalCost:       run.JudgeTotalCost,
			OriginalAvgLatencyMs: run.OriginalAvgLatencyMs,
			ReplayAvgLatencyMs:   run.ReplayAvgLatencyMs,
			EvaluatorSummary:     json.RawMessage(run.EvaluatorSummary),
		}
		ids[i] = run.ID
	}

	if err := rr.post("/api/v1/steward/eval-runs/status", updates); err != nil {
		slog.Warn("results reporter: post eval run statuses failed", "count", len(updates), "error", err)
		return
	}
	if err := rr.store.MarkEvalRunsSynced(ctx, ids); err != nil {
		slog.Warn("results reporter: mark eval runs synced failed", "count", len(ids), "error", err)
	}
}

// flushEvalResults sends eval_results (with their scores nested) to Butler in
// one batch, then marks both the result rows and the score rows as synced.
// Scores added to an already-synced result row (e.g. re-evaluation) are out of
// scope today; if that flow is added, a separate orphan-score sync path is needed.
func (rr *ResultsReporter) flushEvalResults() {
	ctx := context.Background()
	results, err := rr.store.FetchUnsyncedEvalResults(ctx, reporterBatchSize)
	if err != nil {
		slog.Warn("results reporter: fetch eval results failed", "error", err)
		return
	}
	if len(results) == 0 {
		return
	}

	// Collect scores for each result. We fetch all unsynced scores once and
	// bucket them by eval_result_id.
	scores, err := rr.store.FetchUnsyncedEvalResultScores(ctx, reporterBatchSize*4)
	if err != nil {
		slog.Warn("results reporter: fetch eval result scores failed", "error", err)
		return
	}
	scoresByResult := make(map[uuid.UUID][]*models.EvalResultScore, len(results))
	for _, s := range scores {
		scoresByResult[s.EvalResultID] = append(scoresByResult[s.EvalResultID], s)
	}

	payloads := make([]evalResultPayload, len(results))
	resultIDs := make([]uuid.UUID, len(results))
	syncedScoreIDs := make([]uuid.UUID, 0, len(scores))

	for i, res := range results {
		scoreSpecs := make([]evalResultScoreSpec, 0)
		for _, s := range scoresByResult[res.ID] {
			scoreSpecs = append(scoreSpecs, evalResultScoreSpec{
				ID:            s.ID,
				EvaluatorName: s.EvaluatorName,
				Score:         s.Score,
				Reason:        s.Reason,
				CreatedAt:     s.CreatedAt,
			})
			syncedScoreIDs = append(syncedScoreIDs, s.ID)
		}
		payloads[i] = evalResultPayload{
			ID:                   res.ID,
			EvalRunID:            res.EvalRunID,
			SourceRequestID:      res.SourceRequestID,
			OriginalProvider:     res.OriginalProvider,
			OriginalModel:        res.OriginalModel,
			OriginalCost:         res.OriginalCost,
			OriginalLatencyMs:    res.OriginalLatencyMs,
			OriginalInputTokens:  res.OriginalInputTokens,
			OriginalOutputTokens: res.OriginalOutputTokens,
			ReplayResponse:       res.ReplayResponse,
			ReplayCost:           res.ReplayCost,
			ReplayLatencyMs:      res.ReplayLatencyMs,
			ReplayInputTokens:    res.ReplayInputTokens,
			ReplayOutputTokens:   res.ReplayOutputTokens,
			ErrorMessage:         res.ErrorMessage,
			CreatedAt:            res.CreatedAt,
			Scores:               scoreSpecs,
		}
		resultIDs[i] = res.ID
	}

	if err := rr.post("/api/v1/steward/eval-results", payloads); err != nil {
		slog.Warn("results reporter: post eval results failed", "count", len(payloads), "error", err)
		return
	}
	if err := rr.store.MarkEvalResultsSynced(ctx, resultIDs); err != nil {
		slog.Warn("results reporter: mark eval results synced failed", "count", len(resultIDs), "error", err)
	}
	if len(syncedScoreIDs) > 0 {
		if err := rr.store.MarkEvalResultScoresSynced(ctx, syncedScoreIDs); err != nil {
			slog.Warn("results reporter: mark eval result scores synced failed", "count", len(syncedScoreIDs), "error", err)
		}
	}
}

// post sends a JSON body to the given Butler path. Caller resolves the path.
func (rr *ResultsReporter) post(path string, body any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	url := rr.cfg.ButlerBaseURL + path
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+rr.cfg.StewardToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := rr.client.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("butler returned %d for %s", resp.StatusCode, path)
	}
	return nil
}

// --- Wire formats matching Butler's ingest types ---

type replayRunStatusUpdate struct {
	ID                   uuid.UUID  `json:"id"`
	Status               string     `json:"status"`
	ErrorMessage         *string    `json:"error_message,omitempty"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	CompletedAt          *time.Time `json:"completed_at,omitempty"`
	TotalRequests        *int       `json:"total_requests,omitempty"`
	ExactMatches         *int       `json:"exact_matches,omitempty"`
	JudgeEquivalent      *int       `json:"judge_equivalent,omitempty"`
	Divergent            *int       `json:"divergent,omitempty"`
	OriginalTotalCost    *float64   `json:"original_total_cost,omitempty"`
	ReplayTotalCost      *float64   `json:"replay_total_cost,omitempty"`
	OriginalAvgLatencyMs *int       `json:"original_avg_latency_ms,omitempty"`
	ReplayAvgLatencyMs   *int       `json:"replay_avg_latency_ms,omitempty"`
}

type replayResultPayload struct {
	ID                   uuid.UUID `json:"id"`
	ReplayRunID          uuid.UUID `json:"replay_run_id"`
	SourceRequestID      uuid.UUID `json:"source_request_id"`
	OriginalProvider     string    `json:"original_provider"`
	OriginalModel        string    `json:"original_model"`
	OriginalCost         float64   `json:"original_cost"`
	OriginalLatencyMs    int       `json:"original_latency_ms"`
	OriginalInputTokens  int       `json:"original_input_tokens"`
	OriginalOutputTokens int       `json:"original_output_tokens"`
	ReplayResponse       *string   `json:"replay_response,omitempty"`
	ReplayCost           *float64  `json:"replay_cost,omitempty"`
	ReplayLatencyMs      *int      `json:"replay_latency_ms,omitempty"`
	ReplayInputTokens    *int      `json:"replay_input_tokens,omitempty"`
	ReplayOutputTokens   *int      `json:"replay_output_tokens,omitempty"`
	ExactMatch           *bool     `json:"exact_match,omitempty"`
	JudgeEquivalent      *bool     `json:"judge_equivalent,omitempty"`
	JudgeReason          *string   `json:"judge_reason,omitempty"`
	ErrorMessage         *string   `json:"error_message,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
}

type evalRunStatusUpdate struct {
	ID                   uuid.UUID       `json:"id"`
	Status               string          `json:"status"`
	ErrorMessage         *string         `json:"error_message,omitempty"`
	StartedAt            *time.Time      `json:"started_at,omitempty"`
	CompletedAt          *time.Time      `json:"completed_at,omitempty"`
	TotalRequests        *int            `json:"total_requests,omitempty"`
	SuccessfulRequests   *int            `json:"successful_requests,omitempty"`
	FailedRequests       *int            `json:"failed_requests,omitempty"`
	OriginalTotalCost    *float64        `json:"original_total_cost,omitempty"`
	ReplayTotalCost      *float64        `json:"replay_total_cost,omitempty"`
	JudgeTotalCost       *float64        `json:"judge_total_cost,omitempty"`
	OriginalAvgLatencyMs *int            `json:"original_avg_latency_ms,omitempty"`
	ReplayAvgLatencyMs   *int            `json:"replay_avg_latency_ms,omitempty"`
	EvaluatorSummary     json.RawMessage `json:"evaluator_summary,omitempty"`
}

type evalResultPayload struct {
	ID                   uuid.UUID             `json:"id"`
	EvalRunID            uuid.UUID             `json:"eval_run_id,omitempty"`
	SourceRequestID      uuid.UUID             `json:"source_request_id,omitempty"`
	OriginalProvider     string                `json:"original_provider,omitempty"`
	OriginalModel        string                `json:"original_model,omitempty"`
	OriginalCost         float64               `json:"original_cost,omitempty"`
	OriginalLatencyMs    int                   `json:"original_latency_ms,omitempty"`
	OriginalInputTokens  int                   `json:"original_input_tokens,omitempty"`
	OriginalOutputTokens int                   `json:"original_output_tokens,omitempty"`
	ReplayResponse       *string               `json:"replay_response,omitempty"`
	ReplayCost           *float64              `json:"replay_cost,omitempty"`
	ReplayLatencyMs      *int                  `json:"replay_latency_ms,omitempty"`
	ReplayInputTokens    *int                  `json:"replay_input_tokens,omitempty"`
	ReplayOutputTokens   *int                  `json:"replay_output_tokens,omitempty"`
	ErrorMessage         *string               `json:"error_message,omitempty"`
	CreatedAt            time.Time             `json:"created_at,omitempty"`
	Scores               []evalResultScoreSpec `json:"scores"`
}

type evalResultScoreSpec struct {
	ID            uuid.UUID `json:"id"`
	EvaluatorName string    `json:"evaluator_name"`
	Score         float64   `json:"score"`
	Reason        *string   `json:"reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}
