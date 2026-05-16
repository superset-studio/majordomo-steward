package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// EvalStorage is the interface satisfied by EvalRepository.
type EvalStorage interface {
	// Downstream sync (received from Butler)
	UpsertEvalSet(ctx context.Context, set *models.EvalSet) error
	UpsertEvalSetItem(ctx context.Context, item *models.EvalSetItem) error
	UpsertEvalRun(ctx context.Context, run *models.EvalRun) error

	// Worker reads
	GetEvalSet(ctx context.Context, id uuid.UUID) (*models.EvalSet, error)
	GetEvalRun(ctx context.Context, id uuid.UUID) (*models.EvalRun, error)
	ListEvalSetItems(ctx context.Context, evalSetID uuid.UUID) ([]*models.EvalSetItem, error)
	ClaimPendingEvalRun(ctx context.Context) (*models.EvalRun, error)

	// Worker writes
	UpdateEvalRunStatus(ctx context.Context, id uuid.UUID, status string, errorMessage *string) error
	UpdateEvalRunSummary(ctx context.Context, id uuid.UUID, summary *models.EvalRun) error
	InsertEvalResult(ctx context.Context, result *models.EvalResult) error
	InsertEvalResultScore(ctx context.Context, score *models.EvalResultScore) error

	// Upstream sync (to Butler)
	FetchUnsyncedEvalRuns(ctx context.Context, limit int) ([]*models.EvalRun, error)
	MarkEvalRunsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedEvalResults(ctx context.Context, limit int) ([]*models.EvalResult, error)
	MarkEvalResultsSynced(ctx context.Context, ids []uuid.UUID) error
	FetchUnsyncedEvalResultScores(ctx context.Context, limit int) ([]*models.EvalResultScore, error)
	MarkEvalResultScoresSynced(ctx context.Context, ids []uuid.UUID) error
}

// EvalRepository handles eval set, run, result, and score data access on Steward.
// Sets, items, and runs are received from Butler via downstream sync; results
// and scores are written by the local worker and synced upstream to Butler.
type EvalRepository struct {
	db *sqlx.DB
}

// NewEvalRepository constructs an EvalRepository backed by the given database.
func NewEvalRepository(db *sqlx.DB) *EvalRepository {
	return &EvalRepository{db: db}
}

const evalSetColumns = `id, user_id, org_id, name, description, created_at, updated_at`

const evalSetItemColumns = `id, eval_set_id, request_id, created_at`

const evalRunColumns = `id, user_id, org_id, eval_set_id, status, error_message,
	target_provider, target_model, evaluators, evaluator_summary,
	total_requests, successful_requests, failed_requests,
	original_total_cost, replay_total_cost, judge_total_cost,
	original_avg_latency_ms, replay_avg_latency_ms,
	started_at, completed_at, created_at, updated_at, synced_at`

const evalResultColumns = `id, eval_run_id, source_request_id,
	original_provider, original_model, original_cost, original_latency_ms,
	original_input_tokens, original_output_tokens,
	replay_response, replay_cost, replay_latency_ms,
	replay_input_tokens, replay_output_tokens,
	error_message, created_at, synced_at`

const evalResultScoreColumns = `id, eval_result_id, evaluator_name, score, reason, created_at, synced_at`

// --- Downstream sync (received from Butler) ---

// UpsertEvalSet inserts or updates an eval set received from Butler.
func (r *EvalRepository) UpsertEvalSet(ctx context.Context, set *models.EvalSet) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO eval_sets (id, user_id, org_id, name, description, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), now())
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			description = EXCLUDED.description,
			updated_at = now()`,
		set.ID, set.UserID, set.OrgID, set.Name, set.Description, nullableTime(set.CreatedAt))
	if err != nil {
		return fmt.Errorf("upsert eval set: %w", err)
	}
	return nil
}

// UpsertEvalSetItem inserts an eval set item received from Butler. Duplicates are ignored.
func (r *EvalRepository) UpsertEvalSetItem(ctx context.Context, item *models.EvalSetItem) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO eval_set_items (id, eval_set_id, request_id, created_at)
		VALUES ($1, $2, $3, COALESCE($4, now()))
		ON CONFLICT (eval_set_id, request_id) DO NOTHING`,
		item.ID, item.EvalSetID, item.RequestID, nullableTime(item.CreatedAt))
	if err != nil {
		return fmt.Errorf("upsert eval set item: %w", err)
	}
	return nil
}

// UpsertEvalRun inserts or updates an eval run received from Butler. Local
// mutations (status, summary, synced_at) are preserved on conflict.
func (r *EvalRepository) UpsertEvalRun(ctx context.Context, run *models.EvalRun) error {
	var evaluators *string
	if len(run.Evaluators) > 0 {
		str := string(run.Evaluators)
		evaluators = &str
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO eval_runs (
			id, user_id, org_id, eval_set_id, status,
			target_provider, target_model, evaluators,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, COALESCE($8::jsonb, '[]'::jsonb),
			COALESCE($9, now()), now()
		)
		ON CONFLICT (id) DO UPDATE SET
			eval_set_id = EXCLUDED.eval_set_id,
			target_provider = EXCLUDED.target_provider,
			target_model = EXCLUDED.target_model,
			evaluators = EXCLUDED.evaluators,
			updated_at = now()`,
		run.ID, run.UserID, run.OrgID, run.EvalSetID, run.Status,
		run.TargetProvider, run.TargetModel, evaluators,
		nullableTime(run.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("upsert eval run: %w", err)
	}
	return nil
}

// --- Worker reads ---

// GetEvalSet returns an eval set by ID. Returns ErrEvalSetNotFound when not found.
func (r *EvalRepository) GetEvalSet(ctx context.Context, id uuid.UUID) (*models.EvalSet, error) {
	var set models.EvalSet
	err := r.db.GetContext(ctx, &set,
		`SELECT `+evalSetColumns+`,
		(SELECT COUNT(*) FROM eval_set_items WHERE eval_set_id = eval_sets.id) AS item_count
		FROM eval_sets WHERE id = $1`, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEvalSetNotFound
		}
		return nil, fmt.Errorf("get eval set: %w", err)
	}
	return &set, nil
}

// GetEvalRun returns an eval run by ID. Returns ErrEvalRunNotFound when not found.
func (r *EvalRepository) GetEvalRun(ctx context.Context, id uuid.UUID) (*models.EvalRun, error) {
	var run models.EvalRun
	err := r.db.GetContext(ctx, &run, `SELECT `+evalRunColumns+` FROM eval_runs WHERE id = $1`, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEvalRunNotFound
		}
		return nil, fmt.Errorf("get eval run: %w", err)
	}
	run.ParseJSONFields()
	return &run, nil
}

// ListEvalSetItems returns every item in an eval set, ordered by created_at.
func (r *EvalRepository) ListEvalSetItems(ctx context.Context, evalSetID uuid.UUID) ([]*models.EvalSetItem, error) {
	var items []*models.EvalSetItem
	if err := r.db.SelectContext(ctx, &items,
		`SELECT `+evalSetItemColumns+` FROM eval_set_items WHERE eval_set_id = $1 ORDER BY created_at`,
		evalSetID); err != nil {
		return nil, fmt.Errorf("list eval set items: %w", err)
	}
	return items, nil
}

// ClaimPendingEvalRun atomically picks the oldest pending eval run, marks
// it running, and returns it. Returns (nil, nil) when no work is available.
func (r *EvalRepository) ClaimPendingEvalRun(ctx context.Context) (*models.EvalRun, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var run models.EvalRun
	err = tx.GetContext(ctx, &run, `
		SELECT `+evalRunColumns+`
		FROM eval_runs
		WHERE status = 'pending'
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select pending eval run: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE eval_runs SET status = 'running', started_at = now(), updated_at = now()
		WHERE id = $1`, run.ID); err != nil {
		return nil, fmt.Errorf("claim eval run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim eval run: %w", err)
	}

	run.Status = "running"
	run.ParseJSONFields()
	return &run, nil
}

// --- Worker writes ---

// UpdateEvalRunStatus updates the status (and related timestamps/error) of an eval run.
func (r *EvalRepository) UpdateEvalRunStatus(ctx context.Context, id uuid.UUID, status string, errorMessage *string) error {
	var query string
	var args []interface{}

	switch status {
	case "running":
		query = `UPDATE eval_runs SET status = $2, started_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	case "completed":
		query = `UPDATE eval_runs SET status = $2, completed_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	case "failed":
		query = `UPDATE eval_runs SET status = $2, error_message = $3, completed_at = now(), updated_at = now() WHERE id = $1`
		args = []interface{}{id, status, errorMessage}
	default:
		query = `UPDATE eval_runs SET status = $2, updated_at = now() WHERE id = $1`
		args = []interface{}{id, status}
	}

	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update eval run status: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrEvalRunNotFound
	}
	return nil
}

// UpdateEvalRunSummary persists aggregate metrics and evaluator_summary
// computed at the end of a run.
func (r *EvalRepository) UpdateEvalRunSummary(ctx context.Context, id uuid.UUID, summary *models.EvalRun) error {
	var evaluatorSummary *string
	if len(summary.EvaluatorSummary) > 0 {
		str := string(summary.EvaluatorSummary)
		evaluatorSummary = &str
	}

	result, err := r.db.ExecContext(ctx, `
		UPDATE eval_runs SET
			total_requests = $2,
			successful_requests = $3,
			failed_requests = $4,
			original_total_cost = $5,
			replay_total_cost = $6,
			judge_total_cost = $7,
			original_avg_latency_ms = $8,
			replay_avg_latency_ms = $9,
			evaluator_summary = $10::jsonb,
			updated_at = now()
		WHERE id = $1`,
		id,
		summary.TotalRequests, summary.SuccessfulRequests, summary.FailedRequests,
		summary.OriginalTotalCost, summary.ReplayTotalCost, summary.JudgeTotalCost,
		summary.OriginalAvgLatencyMs, summary.ReplayAvgLatencyMs,
		evaluatorSummary,
	)
	if err != nil {
		return fmt.Errorf("update eval run summary: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrEvalRunNotFound
	}
	return nil
}

// InsertEvalResult inserts a single eval result row.
func (r *EvalRepository) InsertEvalResult(ctx context.Context, result *models.EvalResult) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO eval_results (
			id, eval_run_id, source_request_id,
			original_provider, original_model, original_cost, original_latency_ms,
			original_input_tokens, original_output_tokens,
			replay_response, replay_cost, replay_latency_ms,
			replay_input_tokens, replay_output_tokens,
			error_message
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7,
			$8, $9,
			$10, $11, $12,
			$13, $14,
			$15
		)`,
		result.ID, result.EvalRunID, result.SourceRequestID,
		result.OriginalProvider, result.OriginalModel, result.OriginalCost, result.OriginalLatencyMs,
		result.OriginalInputTokens, result.OriginalOutputTokens,
		result.ReplayResponse, result.ReplayCost, result.ReplayLatencyMs,
		result.ReplayInputTokens, result.ReplayOutputTokens,
		result.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("insert eval result: %w", err)
	}
	return nil
}

// InsertEvalResultScore inserts a single evaluator score for an eval result.
// Duplicates on (eval_result_id, evaluator_name) update score and reason.
func (r *EvalRepository) InsertEvalResultScore(ctx context.Context, score *models.EvalResultScore) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO eval_result_scores (id, eval_result_id, evaluator_name, score, reason)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (eval_result_id, evaluator_name) DO UPDATE
		SET score = EXCLUDED.score, reason = EXCLUDED.reason, synced_at = NULL`,
		score.ID, score.EvalResultID, score.EvaluatorName, score.Score, score.Reason)
	if err != nil {
		return fmt.Errorf("insert eval result score: %w", err)
	}
	return nil
}

// --- Upstream sync (to Butler) ---

// FetchUnsyncedEvalRuns returns eval runs with status/summary changes not yet
// synced to Butler, ordered by updated_at ascending.
func (r *EvalRepository) FetchUnsyncedEvalRuns(ctx context.Context, limit int) ([]*models.EvalRun, error) {
	var runs []*models.EvalRun
	err := r.db.SelectContext(ctx, &runs, `
		SELECT `+evalRunColumns+` FROM eval_runs
		WHERE synced_at IS NULL OR synced_at < updated_at
		ORDER BY updated_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced eval runs: %w", err)
	}
	for _, run := range runs {
		run.ParseJSONFields()
	}
	return runs, nil
}

// MarkEvalRunsSynced sets synced_at = now() on the given run IDs.
func (r *EvalRepository) MarkEvalRunsSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE eval_runs SET synced_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark eval runs synced: %w", err)
	}
	return nil
}

// FetchUnsyncedEvalResults returns eval results not yet synced to Butler,
// ordered by created_at ascending.
func (r *EvalRepository) FetchUnsyncedEvalResults(ctx context.Context, limit int) ([]*models.EvalResult, error) {
	var results []*models.EvalResult
	err := r.db.SelectContext(ctx, &results, `
		SELECT `+evalResultColumns+` FROM eval_results
		WHERE synced_at IS NULL
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced eval results: %w", err)
	}
	return results, nil
}

// MarkEvalResultsSynced sets synced_at = now() on the given result IDs.
func (r *EvalRepository) MarkEvalResultsSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE eval_results SET synced_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark eval results synced: %w", err)
	}
	return nil
}

// FetchUnsyncedEvalResultScores returns eval result scores not yet synced to
// Butler, ordered by created_at ascending.
func (r *EvalRepository) FetchUnsyncedEvalResultScores(ctx context.Context, limit int) ([]*models.EvalResultScore, error) {
	var scores []*models.EvalResultScore
	err := r.db.SelectContext(ctx, &scores, `
		SELECT `+evalResultScoreColumns+` FROM eval_result_scores
		WHERE synced_at IS NULL
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unsynced eval result scores: %w", err)
	}
	return scores, nil
}

// MarkEvalResultScoresSynced sets synced_at = now() on the given score IDs.
func (r *EvalRepository) MarkEvalResultScoresSynced(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `UPDATE eval_result_scores SET synced_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark eval result scores synced: %w", err)
	}
	return nil
}

// nullableTime returns nil for the zero value of time.Time so callers can rely
// on COALESCE($N, now()) for insert-time defaults.
func nullableTime(t interface{}) interface{} {
	if v, ok := t.(interface{ IsZero() bool }); ok && v.IsZero() {
		return nil
	}
	return t
}
