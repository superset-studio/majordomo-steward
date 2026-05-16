package repositories

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// ReplayStorage is the interface satisfied by ReplayRepository.
type ReplayStorage interface {
	UpsertReplayRun(ctx context.Context, run *models.ReplayRun) error
	GetReplayRun(ctx context.Context, id uuid.UUID) (*models.ReplayRun, error)
	ClaimPendingReplayRun(ctx context.Context) (*models.ReplayRun, error)
	UpdateReplayRunStatus(ctx context.Context, id uuid.UUID, status string, errorMessage *string) error
	UpdateReplayRunSummary(ctx context.Context, id uuid.UUID, summary *models.ReplayRun) error
	InsertReplayResult(ctx context.Context, result *models.ReplayResult) error
	FetchUnsyncedReplayRuns(ctx context.Context, limit int) ([]*models.ReplayRun, error)
	MarkReplayRunsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedReplayResults(ctx context.Context, limit int) ([]*models.ReplayResult, error)
	MarkReplayResultsSynced(ctx context.Context, ids []uuid.UUID) error
}

// ReplayRepository handles replay run and result data access on Steward.
// Runs are received from Butler via downstream sync; results are written by
// the local worker and synced upstream to Butler.
type ReplayRepository struct {
	db *sqlx.DB
}

// NewReplayRepository constructs a ReplayRepository backed by the given database.
func NewReplayRepository(db *sqlx.DB) *ReplayRepository {
	return &ReplayRepository{db: db}
}

const replayRunColumns = `id, user_id, org_id, status, error_message,
	source_api_key_id, source_provider, source_model, source_start, source_end, source_metadata, source_limit,
	target_provider, target_model,
	judge_enabled, judge_provider, judge_model,
	total_requests, exact_matches, judge_equivalent, divergent,
	original_total_cost, replay_total_cost, original_avg_latency_ms, replay_avg_latency_ms,
	started_at, completed_at, created_at, updated_at, synced_at`

const replayResultColumns = `id, replay_run_id, source_request_id,
	original_provider, original_model, original_cost, original_latency_ms, original_input_tokens, original_output_tokens,
	replay_response, replay_cost, replay_latency_ms, replay_input_tokens, replay_output_tokens,
	exact_match, judge_equivalent, judge_reason, error_message, created_at, synced_at`

// UpsertReplayRun inserts or updates a replay run record received from Butler.
// Existing local mutations (status, summary, synced_at) are preserved on conflict.
func (r *ReplayRepository) UpsertReplayRun(ctx context.Context, run *models.ReplayRun) error {
	var sourceMetadata *string
	if len(run.SourceMetadata) > 0 {
		str := string(run.SourceMetadata)
		sourceMetadata = &str
	}

	query := `
		INSERT INTO replay_runs (
			id, user_id, org_id, status,
			source_api_key_id, source_provider, source_model, source_start, source_end, source_metadata, source_limit,
			target_provider, target_model,
			judge_enabled, judge_provider, judge_model,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9, $10, $11,
			$12, $13,
			$14, $15, $16,
			COALESCE($17, now()), now()
		)
		ON CONFLICT (id) DO UPDATE SET
			source_api_key_id = EXCLUDED.source_api_key_id,
			source_provider = EXCLUDED.source_provider,
			source_model = EXCLUDED.source_model,
			source_start = EXCLUDED.source_start,
			source_end = EXCLUDED.source_end,
			source_metadata = EXCLUDED.source_metadata,
			source_limit = EXCLUDED.source_limit,
			target_provider = EXCLUDED.target_provider,
			target_model = EXCLUDED.target_model,
			judge_enabled = EXCLUDED.judge_enabled,
			judge_provider = EXCLUDED.judge_provider,
			judge_model = EXCLUDED.judge_model,
			updated_at = now()`

	var createdAt interface{}
	if !run.CreatedAt.IsZero() {
		createdAt = run.CreatedAt
	}

	_, err := r.db.ExecContext(ctx, query,
		run.ID, run.UserID, run.OrgID, run.Status,
		run.SourceAPIKeyID, run.SourceProvider, run.SourceModel,
		run.SourceStart, run.SourceEnd, sourceMetadata, run.SourceLimit,
		run.TargetProvider, run.TargetModel,
		run.JudgeEnabled, run.JudgeProvider, run.JudgeModel,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("upsert replay run: %w", err)
	}
	return nil
}

// GetReplayRun returns a replay run by ID. Returns ErrReplayRunNotFound when not found.
func (r *ReplayRepository) GetReplayRun(ctx context.Context, id uuid.UUID) (*models.ReplayRun, error) {
	var run models.ReplayRun
	err := r.db.GetContext(ctx, &run, `SELECT `+replayRunColumns+` FROM replay_runs WHERE id = $1`, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrReplayRunNotFound
		}
		return nil, fmt.Errorf("get replay run: %w", err)
	}
	parseReplayRunMetadata(&run)
	return &run, nil
}

// ClaimPendingReplayRun atomically picks the oldest pending replay run, marks
// it running, and returns it. Returns (nil, nil) when no work is available.
func (r *ReplayRepository) ClaimPendingReplayRun(ctx context.Context) (*models.ReplayRun, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var run models.ReplayRun
	err = tx.GetContext(ctx, &run, `
		SELECT `+replayRunColumns+`
		FROM replay_runs
		WHERE status = 'pending'
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select pending replay run: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE replay_runs SET status = 'running', started_at = now(), updated_at = now()
		WHERE id = $1`, run.ID); err != nil {
		return nil, fmt.Errorf("claim replay run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim replay run: %w", err)
	}

	run.Status = "running"
	parseReplayRunMetadata(&run)
	return &run, nil
}

// UpdateReplayRunStatus updates the status (and related timestamps/error) of a replay run.
func (r *ReplayRepository) UpdateReplayRunStatus(ctx context.Context, id uuid.UUID, status string, errorMessage *string) error {
	var query string
	var args []interface{}

	switch status {
	case "running":
		query = `UPDATE replay_runs SET status = $2, started_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	case "completed":
		query = `UPDATE replay_runs SET status = $2, completed_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	case "failed":
		query = `UPDATE replay_runs SET status = $2, error_message = $3, completed_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status, errorMessage}
	default:
		query = `UPDATE replay_runs SET status = $2, updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	}

	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update replay run status: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrReplayRunNotFound
	}
	return nil
}

// UpdateReplayRunSummary persists the aggregate metrics computed at the end of a run.
func (r *ReplayRepository) UpdateReplayRunSummary(ctx context.Context, id uuid.UUID, summary *models.ReplayRun) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE replay_runs SET
			total_requests = $2,
			exact_matches = $3,
			judge_equivalent = $4,
			divergent = $5,
			original_total_cost = $6,
			replay_total_cost = $7,
			original_avg_latency_ms = $8,
			replay_avg_latency_ms = $9,
			updated_at = now()
		WHERE id = $1`,
		id,
		summary.TotalRequests, summary.ExactMatches, summary.JudgeEquivalent, summary.Divergent,
		summary.OriginalTotalCost, summary.ReplayTotalCost,
		summary.OriginalAvgLatencyMs, summary.ReplayAvgLatencyMs,
	)
	if err != nil {
		return fmt.Errorf("update replay run summary: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrReplayRunNotFound
	}
	return nil
}

// InsertReplayResult inserts a single replay result row.
func (r *ReplayRepository) InsertReplayResult(ctx context.Context, result *models.ReplayResult) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO replay_results (
			replay_run_id, source_request_id,
			original_provider, original_model, original_cost, original_latency_ms,
			original_input_tokens, original_output_tokens,
			replay_response, replay_cost, replay_latency_ms, replay_input_tokens, replay_output_tokens,
			exact_match, judge_equivalent, judge_reason, error_message
		) VALUES (
			$1, $2,
			$3, $4, $5, $6,
			$7, $8,
			$9, $10, $11, $12, $13,
			$14, $15, $16, $17
		)`,
		result.ReplayRunID, result.SourceRequestID,
		result.OriginalProvider, result.OriginalModel, result.OriginalCost, result.OriginalLatencyMs,
		result.OriginalInputTokens, result.OriginalOutputTokens,
		result.ReplayResponse, result.ReplayCost, result.ReplayLatencyMs, result.ReplayInputTokens, result.ReplayOutputTokens,
		result.ExactMatch, result.JudgeEquivalent, result.JudgeReason, result.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("insert replay result: %w", err)
	}
	return nil
}

// FetchUnsyncedReplayRuns returns replay runs whose status/summary has been
// updated since the last sync to Butler, ordered by updated_at ascending.
func (r *ReplayRepository) FetchUnsyncedReplayRuns(ctx context.Context, limit int) ([]*models.ReplayRun, error) {
	var runs []*models.ReplayRun
	err := r.db.SelectContext(ctx, &runs, `
		SELECT `+replayRunColumns+` FROM replay_runs
		WHERE synced_at IS NULL OR synced_at < updated_at
		ORDER BY updated_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced replay runs: %w", err)
	}
	for _, run := range runs {
		parseReplayRunMetadata(run)
	}
	return runs, nil
}

// MarkReplayRunsSynced sets synced_at = now() on the given run IDs.
func (r *ReplayRepository) MarkReplayRunsSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE replay_runs SET synced_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark replay runs synced: %w", err)
	}
	return nil
}

// FetchUnsyncedReplayResults returns replay results not yet synced to Butler,
// ordered by created_at ascending.
func (r *ReplayRepository) FetchUnsyncedReplayResults(ctx context.Context, limit int) ([]*models.ReplayResult, error) {
	var results []*models.ReplayResult
	err := r.db.SelectContext(ctx, &results, `
		SELECT `+replayResultColumns+` FROM replay_results
		WHERE synced_at IS NULL
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced replay results: %w", err)
	}
	return results, nil
}

// MarkReplayResultsSynced sets synced_at = now() on the given result IDs.
func (r *ReplayRepository) MarkReplayResultsSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE replay_results SET synced_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark replay results synced: %w", err)
	}
	return nil
}

// parseReplayRunMetadata converts the raw JSONB source_metadata into the parsed map.
func parseReplayRunMetadata(run *models.ReplayRun) {
	if run.SourceMetadata != nil {
		m := make(map[string]string)
		_ = json.Unmarshal(run.SourceMetadata, &m)
		run.SourceMetadataParsed = m
	}
}
