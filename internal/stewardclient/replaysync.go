package stewardclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/models"
)

// replayRunPayload mirrors Butler's ingest.ReplayRunPayload wire format.
type replayRunPayload struct {
	ID           uuid.UUID  `json:"id"`
	UserID       uuid.UUID  `json:"user_id"`
	OrgID        *uuid.UUID `json:"org_id,omitempty"`
	Status       string     `json:"status"`
	ErrorMessage *string    `json:"error_message,omitempty"`

	SourceAPIKeyID *uuid.UUID      `json:"source_api_key_id,omitempty"`
	SourceProvider *string         `json:"source_provider,omitempty"`
	SourceModel    *string         `json:"source_model,omitempty"`
	SourceStart    *time.Time      `json:"source_start,omitempty"`
	SourceEnd      *time.Time      `json:"source_end,omitempty"`
	SourceMetadata json.RawMessage `json:"source_metadata,omitempty"`
	SourceLimit    int             `json:"source_limit"`

	TargetProvider string `json:"target_provider"`
	TargetModel    string `json:"target_model"`

	JudgeEnabled  bool    `json:"judge_enabled"`
	JudgeProvider *string `json:"judge_provider,omitempty"`
	JudgeModel    *string `json:"judge_model,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// ReplaySyncClient fetches replay run specs from Butler.
type ReplaySyncClient struct {
	cfg    config.StewardConfig
	client *http.Client
}

// NewReplaySyncClient creates a ReplaySyncClient for the given Steward config.
func NewReplaySyncClient(cfg config.StewardConfig) *ReplaySyncClient {
	return &ReplaySyncClient{
		cfg:    cfg,
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// FetchReplayRun retrieves a replay run spec from Butler by ID. Returns
// (nil, nil) if Butler responds with 404.
func (c *ReplaySyncClient) FetchReplayRun(ctx context.Context, runID uuid.UUID) (*models.ReplayRun, error) {
	url := fmt.Sprintf("%s/api/v1/steward/replay-runs/%s", c.cfg.ButlerBaseURL, runID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.StewardToken)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch replay run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var payload replayRunPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode replay run: %w", err)
	}

	return payloadToReplayRun(&payload), nil
}

func payloadToReplayRun(p *replayRunPayload) *models.ReplayRun {
	return &models.ReplayRun{
		ID:             p.ID,
		UserID:         p.UserID,
		OrgID:          p.OrgID,
		Status:         p.Status,
		ErrorMessage:   p.ErrorMessage,
		SourceAPIKeyID: p.SourceAPIKeyID,
		SourceProvider: p.SourceProvider,
		SourceModel:    p.SourceModel,
		SourceStart:    p.SourceStart,
		SourceEnd:      p.SourceEnd,
		SourceMetadata: []byte(p.SourceMetadata),
		SourceLimit:    p.SourceLimit,
		TargetProvider: p.TargetProvider,
		TargetModel:    p.TargetModel,
		JudgeEnabled:   p.JudgeEnabled,
		JudgeProvider:  p.JudgeProvider,
		JudgeModel:     p.JudgeModel,
		CreatedAt:      p.CreatedAt,
	}
}
