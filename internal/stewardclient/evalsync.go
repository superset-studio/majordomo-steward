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

// EvalRunBundle bundles an eval run with its eval set and items for atomic
// downstream upsert by the local worker.
type EvalRunBundle struct {
	Run     *models.EvalRun
	EvalSet *models.EvalSet
	Items   []*models.EvalSetItem
}

// evalRunPayload mirrors Butler's ingest.EvalRunPayload wire format.
type evalRunPayload struct {
	Run     evalRunSpec       `json:"run"`
	EvalSet evalSetSpec       `json:"eval_set"`
	Items   []evalSetItemSpec `json:"items"`
}

type evalRunSpec struct {
	ID           uuid.UUID  `json:"id"`
	UserID       uuid.UUID  `json:"user_id"`
	OrgID        *uuid.UUID `json:"org_id,omitempty"`
	EvalSetID    uuid.UUID  `json:"eval_set_id"`
	Status       string     `json:"status"`
	ErrorMessage *string    `json:"error_message,omitempty"`

	TargetProvider string          `json:"target_provider"`
	TargetModel    string          `json:"target_model"`
	Evaluators     json.RawMessage `json:"evaluators"`

	CreatedAt time.Time `json:"created_at"`
}

type evalSetSpec struct {
	ID          uuid.UUID  `json:"id"`
	UserID      uuid.UUID  `json:"user_id"`
	OrgID       *uuid.UUID `json:"org_id,omitempty"`
	Name        string     `json:"name"`
	Description *string    `json:"description,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

type evalSetItemSpec struct {
	ID        uuid.UUID `json:"id"`
	EvalSetID uuid.UUID `json:"eval_set_id"`
	RequestID uuid.UUID `json:"request_id"`
	CreatedAt time.Time `json:"created_at"`
}

// EvalSyncClient fetches eval run specs from Butler.
type EvalSyncClient struct {
	cfg    config.StewardConfig
	client *http.Client
}

// NewEvalSyncClient creates an EvalSyncClient for the given Steward config.
func NewEvalSyncClient(cfg config.StewardConfig) *EvalSyncClient {
	return &EvalSyncClient{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// FetchEvalRun retrieves an eval run plus its set and items from Butler by ID.
// Returns (nil, nil) if Butler responds with 404.
func (c *EvalSyncClient) FetchEvalRun(ctx context.Context, runID uuid.UUID) (*EvalRunBundle, error) {
	url := fmt.Sprintf("%s/api/v1/steward/eval-runs/%s", c.cfg.ButlerBaseURL, runID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.StewardToken)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch eval run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var payload evalRunPayload
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode eval run: %w", err)
	}

	return payloadToBundle(&payload), nil
}

func payloadToBundle(p *evalRunPayload) *EvalRunBundle {
	items := make([]*models.EvalSetItem, len(p.Items))
	for i, it := range p.Items {
		items[i] = &models.EvalSetItem{
			ID:        it.ID,
			EvalSetID: it.EvalSetID,
			RequestID: it.RequestID,
			CreatedAt: it.CreatedAt,
		}
	}

	return &EvalRunBundle{
		Run: &models.EvalRun{
			ID:             p.Run.ID,
			UserID:         p.Run.UserID,
			OrgID:          p.Run.OrgID,
			EvalSetID:      p.Run.EvalSetID,
			Status:         p.Run.Status,
			ErrorMessage:   p.Run.ErrorMessage,
			TargetProvider: p.Run.TargetProvider,
			TargetModel:    p.Run.TargetModel,
			Evaluators:     []byte(p.Run.Evaluators),
			CreatedAt:      p.Run.CreatedAt,
		},
		EvalSet: &models.EvalSet{
			ID:          p.EvalSet.ID,
			UserID:      p.EvalSet.UserID,
			OrgID:       p.EvalSet.OrgID,
			Name:        p.EvalSet.Name,
			Description: p.EvalSet.Description,
			CreatedAt:   p.EvalSet.CreatedAt,
		},
		Items: items,
	}
}
