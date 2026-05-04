package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/models"
)

// ExperimentStore is the minimal repository interface required by ExperimentRouter.
type ExperimentStore interface {
	ListActiveExperiments(ctx context.Context, orgID uuid.UUID, now time.Time) ([]models.LocalExperiment, error)
}

// ExperimentAssignment holds the result of routing a request through an experiment.
type ExperimentAssignment struct {
	ExperimentID  uuid.UUID
	ArmID         uuid.UUID
	AssignedModel string
	OriginalModel string
}

type cachedExperiments struct {
	experiments []models.LocalExperiment
	fetchedAt   time.Time
}

// ExperimentRouter checks incoming requests against active experiment definitions
// and returns an arm assignment when a match is found.
//
// Results are cached per-org with a 30-second TTL to avoid per-request DB hits.
type ExperimentRouter struct {
	store    ExperimentStore
	cacheTTL time.Duration
	cache    sync.Map // orgID (string) → *cachedExperiments
}

// NewExperimentRouter constructs an ExperimentRouter backed by the given store.
func NewExperimentRouter(store ExperimentStore) *ExperimentRouter {
	return &ExperimentRouter{
		store:    store,
		cacheTTL: 30 * time.Second,
	}
}

// Route checks whether the request matches any active experiment for the org.
// Returns nil when no experiment applies. Returns an ExperimentAssignment when
// a match is found and the model should be overridden.
//
// Matching rules (all must hold):
//  1. Experiment api_key_id is nil (all keys) or equals apiKeyID.
//  2. Every key in metadata_filters has an exact-match value in metadata.
//  3. now is within [starts_at, ends_at].
//  4. experiment status is "active" (guaranteed by the query, double-checked here).
//
// When multiple experiments match, the first one found is used.
func (r *ExperimentRouter) Route(
	ctx context.Context,
	orgID uuid.UUID,
	apiKeyID *uuid.UUID,
	metadata map[string]string,
	requestedModel string,
	now time.Time,
) *ExperimentAssignment {
	experiments := r.getExperiments(ctx, orgID, now)

	for _, exp := range experiments {
		if exp.Status != "active" {
			continue
		}
		if exp.APIKeyID != nil && (apiKeyID == nil || *exp.APIKeyID != *apiKeyID) {
			continue
		}
		if !matchesFilters(exp.MetadataFilters, metadata) {
			continue
		}
		if len(exp.Arms) == 0 {
			continue
		}

		arm := selectArm(exp.Arms, exp.StickyKey, metadata)
		if arm == nil {
			continue
		}

		return &ExperimentAssignment{
			ExperimentID:  exp.ID,
			ArmID:         arm.ID,
			AssignedModel: arm.Model,
			OriginalModel: requestedModel,
		}
	}

	return nil
}

// getExperiments returns the cached experiment list for the org, refreshing from
// the DB when the cache has expired.
func (r *ExperimentRouter) getExperiments(ctx context.Context, orgID uuid.UUID, now time.Time) []models.LocalExperiment {
	key := orgID.String()

	if cached, ok := r.cache.Load(key); ok {
		entry := cached.(*cachedExperiments)
		if time.Since(entry.fetchedAt) < r.cacheTTL {
			return entry.experiments
		}
	}

	experiments, err := r.store.ListActiveExperiments(ctx, orgID, now)
	if err != nil {
		// Return stale cache on error rather than breaking all requests.
		if cached, ok := r.cache.Load(key); ok {
			return cached.(*cachedExperiments).experiments
		}
		return nil
	}

	r.cache.Store(key, &cachedExperiments{experiments: experiments, fetchedAt: time.Now()})
	return experiments
}

// matchesFilters reports whether all filter key-value pairs are present in metadata.
func matchesFilters(filters, metadata map[string]string) bool {
	for k, v := range filters {
		if metadata[k] != v {
			return false
		}
	}
	return true
}

// selectArm picks an arm using weighted random selection.
// When stickyKey is set and the corresponding metadata value is non-empty,
// uses a deterministic hash so the same value always maps to the same arm.
func selectArm(arms []models.LocalExperimentArm, stickyKey *string, metadata map[string]string) *models.LocalExperimentArm {
	total := 0
	for _, a := range arms {
		total += a.Weight
	}
	if total <= 0 {
		return nil
	}

	var n int
	if stickyKey != nil {
		if val := metadata[*stickyKey]; val != "" {
			h := fnv.New32a()
			h.Write([]byte(val))
			n = int(h.Sum32() % uint32(total))
		} else {
			n = rand.Intn(total)
		}
	} else {
		n = rand.Intn(total)
	}

	for i := range arms {
		n -= arms[i].Weight
		if n < 0 {
			return &arms[i]
		}
	}
	// Should not reach here, but return last arm as fallback.
	return &arms[len(arms)-1]
}

// OverrideModel replaces the "model" field in a JSON request body.
// Other fields are preserved. Returns an error if the body is not valid JSON.
func OverrideModel(body []byte, newModel string) ([]byte, error) {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("unmarshal request body for model override: %w", err)
	}
	modelJSON, err := json.Marshal(newModel)
	if err != nil {
		return nil, fmt.Errorf("marshal new model: %w", err)
	}
	req["model"] = json.RawMessage(modelJSON)
	return json.Marshal(req)
}
