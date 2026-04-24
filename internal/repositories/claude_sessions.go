package repositories

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/superset-studio/majordomo-steward/internal/models"
)

// ClaudeSessionStorage is the interface satisfied by ClaudeSessionRepository.
type ClaudeSessionStorage interface {
	CreateClaudeSession(ctx context.Context, session *models.ClaudeSession) error
	EndClaudeSession(ctx context.Context, sessionID uuid.UUID) error
	GetClaudeSession(ctx context.Context, sessionID uuid.UUID) (*models.ClaudeSession, error)
	ListClaudeSessions(ctx context.Context, apiKeyID uuid.UUID, limit, offset int) ([]*models.ClaudeSession, int, error)
	UpdateClaudeSessionStats(ctx context.Context, sessionID uuid.UUID, inputTokens, outputTokens int, cost float64) error
	CreateClaudeRequestDetail(ctx context.Context, detail *models.ClaudeRequestDetail) error
	ListClaudeSessionRequests(ctx context.Context, sessionID uuid.UUID, limit, offset int) ([]*models.ClaudeRequestDetail, int, error)
}

// ClaudeSessionRepository handles Claude Code session and request detail data access.
type ClaudeSessionRepository struct {
	db *sqlx.DB
}

// NewClaudeSessionRepository constructs a ClaudeSessionRepository backed by the given database.
func NewClaudeSessionRepository(db *sqlx.DB) *ClaudeSessionRepository {
	return &ClaudeSessionRepository{db: db}
}

const claudeSessionColumns = `id, majordomo_api_key_id, session_name, started_at, ended_at, total_requests, total_input_tokens, total_output_tokens, total_cost, created_at`
const claudeRequestDetailColumns = `id, llm_request_id, session_id, message_count, user_message_count, assistant_message_count, tool_names, tool_use_count, has_thinking, is_plan_mode, stop_reason, system_prompt_hash, created_at`

func (r *ClaudeSessionRepository) CreateClaudeSession(ctx context.Context, session *models.ClaudeSession) error {
	query := `
		INSERT INTO claude_sessions (id, majordomo_api_key_id, session_name, started_at)
		VALUES ($1, $2, $3, $4)
		RETURNING ` + claudeSessionColumns

	if err := r.db.QueryRowxContext(ctx, query,
		session.ID, session.MajordomoAPIKeyID, session.SessionName, session.StartedAt,
	).StructScan(session); err != nil {
		return fmt.Errorf("create claude session: %w", err)
	}
	return nil
}

func (r *ClaudeSessionRepository) EndClaudeSession(ctx context.Context, sessionID uuid.UUID) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE claude_sessions SET ended_at = now() WHERE id = $1 AND ended_at IS NULL`,
		sessionID)
	if err != nil {
		return fmt.Errorf("end claude session: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("end claude session rows affected: %w", err)
	}
	if n == 0 {
		return ErrClaudeSessionNotFound
	}
	return nil
}

func (r *ClaudeSessionRepository) GetClaudeSession(ctx context.Context, sessionID uuid.UUID) (*models.ClaudeSession, error) {
	query := `SELECT ` + claudeSessionColumns + ` FROM claude_sessions WHERE id = $1`

	var session models.ClaudeSession
	if err := r.db.GetContext(ctx, &session, query, sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrClaudeSessionNotFound
		}
		return nil, fmt.Errorf("get claude session: %w", err)
	}
	return &session, nil
}

func (r *ClaudeSessionRepository) ListClaudeSessions(ctx context.Context, apiKeyID uuid.UUID, limit, offset int) ([]*models.ClaudeSession, int, error) {
	var total int
	if err := r.db.GetContext(ctx, &total,
		`SELECT COUNT(*) FROM claude_sessions WHERE majordomo_api_key_id = $1`, apiKeyID); err != nil {
		return nil, 0, fmt.Errorf("count claude sessions: %w", err)
	}

	query := `SELECT ` + claudeSessionColumns + `
		FROM claude_sessions
		WHERE majordomo_api_key_id = $1
		ORDER BY started_at DESC
		LIMIT $2 OFFSET $3`

	var sessions []*models.ClaudeSession
	if err := r.db.SelectContext(ctx, &sessions, query, apiKeyID, limit, offset); err != nil {
		return nil, 0, fmt.Errorf("list claude sessions: %w", err)
	}
	return sessions, total, nil
}

func (r *ClaudeSessionRepository) UpdateClaudeSessionStats(ctx context.Context, sessionID uuid.UUID, inputTokens, outputTokens int, cost float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE claude_sessions
		SET total_requests = total_requests + 1,
		    total_input_tokens = total_input_tokens + $2,
		    total_output_tokens = total_output_tokens + $3,
		    total_cost = total_cost + $4
		WHERE id = $1`,
		sessionID, inputTokens, outputTokens, cost)
	if err != nil {
		return fmt.Errorf("update claude session stats: %w", err)
	}
	return nil
}

func (r *ClaudeSessionRepository) CreateClaudeRequestDetail(ctx context.Context, detail *models.ClaudeRequestDetail) error {
	query := `
		INSERT INTO claude_request_details (
			id, llm_request_id, session_id, message_count, user_message_count, assistant_message_count,
			tool_names, tool_use_count, has_thinking, is_plan_mode, stop_reason, system_prompt_hash
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

	_, err := r.db.ExecContext(ctx, query,
		detail.ID, detail.LLMRequestID, detail.SessionID,
		detail.MessageCount, detail.UserMessageCount, detail.AssistantMessageCount,
		pq.Array(detail.ToolNames), detail.ToolUseCount,
		detail.HasThinking, detail.IsPlanMode, detail.StopReason, detail.SystemPromptHash,
	)
	if err != nil {
		return fmt.Errorf("create claude request detail: %w", err)
	}
	return nil
}

func (r *ClaudeSessionRepository) ListClaudeSessionRequests(ctx context.Context, sessionID uuid.UUID, limit, offset int) ([]*models.ClaudeRequestDetail, int, error) {
	var total int
	if err := r.db.GetContext(ctx, &total,
		`SELECT COUNT(*) FROM claude_request_details WHERE session_id = $1`, sessionID); err != nil {
		return nil, 0, fmt.Errorf("count claude session requests: %w", err)
	}

	query := `SELECT ` + claudeRequestDetailColumns + `
		FROM claude_request_details
		WHERE session_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3`

	var details []*models.ClaudeRequestDetail
	if err := r.db.SelectContext(ctx, &details, query, sessionID, limit, offset); err != nil {
		return nil, 0, fmt.Errorf("list claude session requests: %w", err)
	}
	return details, total, nil
}
