// Package requestlog provides async writing of request logs to PostgreSQL.
package requestlog

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/superset-studio/majordomo-steward/internal/models"
	"github.com/superset-studio/majordomo-steward/internal/repositories"
)

// Writer asynchronously writes request logs to PostgreSQL, enriching them with
// HLL cardinality data and indexed metadata along the way.
type Writer struct {
	db             *sqlx.DB
	logChan        chan *models.RequestLog
	done           chan struct{}
	activeKeyCache *repositories.ActiveKeysCache
	hllManager     *repositories.HLLManager
	claudeSessions *repositories.ClaudeSessionRepository
}

// New constructs a Writer and starts the background write loop.
func New(
	db *sqlx.DB,
	activeKeyCache *repositories.ActiveKeysCache,
	hllManager *repositories.HLLManager,
	claudeSessions *repositories.ClaudeSessionRepository,
) *Writer {
	w := &Writer{
		db:             db,
		logChan:        make(chan *models.RequestLog, 1000),
		done:           make(chan struct{}),
		activeKeyCache: activeKeyCache,
		hllManager:     hllManager,
		claudeSessions: claudeSessions,
	}
	go w.writeLoop()
	return w
}

// WriteRequestLog enqueues a request log for async writing. Non-blocking: drops
// the log with a warning if the channel is full.
func (w *Writer) WriteRequestLog(_ context.Context, log *models.RequestLog) {
	select {
	case w.logChan <- log:
	default:
		slog.Warn("request log channel full, dropping log", "request_id", log.ID)
	}
}

// Ping verifies database connectivity. Satisfies server.HealthChecker.
func (w *Writer) Ping(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

// Close drains the write channel, flushes HLLs, and closes the database.
func (w *Writer) Close() error {
	close(w.done)
	if w.hllManager != nil {
		w.hllManager.Stop()
	}
	return w.db.Close()
}

func (w *Writer) writeLoop() {
	for {
		select {
		case log := <-w.logChan:
			w.writeLog(log)
		case <-w.done:
			for len(w.logChan) > 0 {
				w.writeLog(<-w.logChan)
			}
			return
		}
	}
}

func (w *Writer) writeLog(log *models.RequestLog) {
	ctx := context.Background()

	var indexedMetadata map[string]string
	if log.MajordomoAPIKeyID != nil {
		activeKeys, _ := w.activeKeyCache.GetActiveKeys(ctx, *log.MajordomoAPIKeyID)
		indexedMetadata = make(map[string]string)
		for key, value := range log.RawMetadata {
			if activeKeys[key] {
				indexedMetadata[key] = value
			}
		}
	} else {
		indexedMetadata = make(map[string]string)
	}

	rawMetadataJSON, err := json.Marshal(log.RawMetadata)
	if err != nil {
		slog.Error("failed to marshal raw metadata", "error", err)
		rawMetadataJSON = []byte("{}")
	}

	indexedMetadataJSON, err := json.Marshal(indexedMetadata)
	if err != nil {
		slog.Error("failed to marshal indexed metadata", "error", err)
		indexedMetadataJSON = []byte("{}")
	}

	query := `
		INSERT INTO llm_requests (
			id, user_id, org_id, majordomo_api_key_id, proxy_key_id, provider_api_key_hash, provider_api_key_alias,
			provider, model, request_path, request_method,
			requested_at, responded_at, response_time_ms,
			input_tokens, output_tokens, cached_tokens, cache_creation_tokens,
			input_cost, output_cost, total_cost,
			status_code, error_message, raw_metadata, indexed_metadata,
			request_body, response_body, body_s3_key, model_alias_found
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29
		)`

	_, err = w.db.ExecContext(ctx, query,
		log.ID, log.UserID, log.OrgID, log.MajordomoAPIKeyID, log.ProxyKeyID, log.ProviderAPIKeyHash, log.ProviderAPIKeyAlias,
		log.Provider, log.Model, log.RequestPath, log.RequestMethod,
		log.RequestedAt, log.RespondedAt, log.ResponseTimeMs,
		log.InputTokens, log.OutputTokens, log.CachedTokens, log.CacheCreationTokens,
		log.InputCost, log.OutputCost, log.TotalCost,
		log.StatusCode, log.ErrorMessage, rawMetadataJSON, indexedMetadataJSON,
		log.RequestBody, log.ResponseBody, log.BodyS3Key, log.ModelAliasFound,
	)
	if err != nil {
		slog.Error("failed to write request log", "error", err, "request_id", log.ID)
		return
	}

	if log.MajordomoAPIKeyID != nil {
		for key, value := range log.RawMetadata {
			w.hllManager.AddValue(*log.MajordomoAPIKeyID, key, value)
		}
		w.registerMetadataKeys(ctx, *log.MajordomoAPIKeyID, log.RawMetadata)
	}

	if log.ClaudeMetadata != nil {
		m := log.ClaudeMetadata
		detail := &models.ClaudeRequestDetail{
			ID:                    uuid.New(),
			LLMRequestID:          log.ID,
			SessionID:             m.SessionID,
			MessageCount:          m.MessageCount,
			UserMessageCount:      m.UserMessageCount,
			AssistantMessageCount: m.AssistantMessageCount,
			ToolNames:             m.ToolNames,
			ToolUseCount:          m.ToolUseCount,
			HasThinking:           m.HasThinking,
			IsPlanMode:            m.IsPlanMode,
		}
		if m.StopReason != "" {
			detail.StopReason = &m.StopReason
		}
		if m.SystemPromptHash != "" {
			detail.SystemPromptHash = &m.SystemPromptHash
		}
		if err := w.claudeSessions.CreateClaudeRequestDetail(ctx, detail); err != nil {
			slog.Error("failed to create claude request detail", "error", err, "request_id", log.ID)
		}
		if m.SessionID != nil {
			if err := w.claudeSessions.UpdateClaudeSessionStats(ctx, *m.SessionID, log.InputTokens, log.OutputTokens, log.TotalCost); err != nil {
				slog.Error("failed to update claude session stats", "error", err, "session_id", m.SessionID)
			}
		}
	}
}

func (w *Writer) registerMetadataKeys(ctx context.Context, apiKeyID uuid.UUID, metadata map[string]string) {
	if len(metadata) == 0 {
		return
	}

	query := `
		INSERT INTO llm_requests_metadata_keys (majordomo_api_key_id, key_name)
		VALUES ($1, $2)
		ON CONFLICT (majordomo_api_key_id, key_name) DO NOTHING`

	for key := range metadata {
		if _, err := w.db.ExecContext(ctx, query, apiKeyID, key); err != nil {
			slog.Warn("failed to register metadata key", "error", err, "key", key)
		}
	}
}
