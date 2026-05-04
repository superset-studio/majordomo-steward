package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/models"
)

// ── mock store ────────────────────────────────────────────────────────────────

type mockExperimentStore struct {
	experiments []models.LocalExperiment
	err         error
}

func (m *mockExperimentStore) ListActiveExperiments(_ context.Context, _ uuid.UUID, _ time.Time) ([]models.LocalExperiment, error) {
	return m.experiments, m.err
}

// ── helpers ───────────────────────────────────────────────────────────────────

func strPtr(s string) *string   { return &s }
func uuidPtr(id uuid.UUID) *uuid.UUID { return &id }

func makeActiveExperiment(apiKeyID *uuid.UUID, filters map[string]string, stickyKey *string, arms []models.LocalExperimentArm) models.LocalExperiment {
	return models.LocalExperiment{
		ID:              uuid.New(),
		Status:          "active",
		APIKeyID:        apiKeyID,
		MetadataFilters: filters,
		StickyKey:       stickyKey,
		StartsAt:        time.Now().Add(-time.Hour),
		EndsAt:          time.Now().Add(time.Hour),
		Arms:            arms,
	}
}

func twoArms() []models.LocalExperimentArm {
	return []models.LocalExperimentArm{
		{ID: uuid.New(), Model: "gpt-4o", Weight: 50, IsControl: true},
		{ID: uuid.New(), Model: "claude-3-5-sonnet-20241022", Weight: 50, IsControl: false},
	}
}

// ── TestMatchesFilters ────────────────────────────────────────────────────────

func TestMatchesFilters(t *testing.T) {
	tests := []struct {
		name     string
		filters  map[string]string
		metadata map[string]string
		want     bool
	}{
		{
			name:     "nil filters always match",
			filters:  nil,
			metadata: map[string]string{"env": "prod"},
			want:     true,
		},
		{
			name:     "empty filters always match",
			filters:  map[string]string{},
			metadata: map[string]string{"env": "prod"},
			want:     true,
		},
		{
			name:     "empty filters match empty metadata",
			filters:  map[string]string{},
			metadata: map[string]string{},
			want:     true,
		},
		{
			name:     "single filter exact match",
			filters:  map[string]string{"env": "prod"},
			metadata: map[string]string{"env": "prod", "user": "alice"},
			want:     true,
		},
		{
			name:     "single filter wrong value",
			filters:  map[string]string{"env": "prod"},
			metadata: map[string]string{"env": "dev"},
			want:     false,
		},
		{
			name:     "single filter key missing from metadata",
			filters:  map[string]string{"env": "prod"},
			metadata: map[string]string{"user": "alice"},
			want:     false,
		},
		{
			name:     "filters against empty metadata",
			filters:  map[string]string{"env": "prod"},
			metadata: map[string]string{},
			want:     false,
		},
		{
			name:     "all filters must match — all present and correct",
			filters:  map[string]string{"env": "prod", "tier": "enterprise"},
			metadata: map[string]string{"env": "prod", "tier": "enterprise", "user": "alice"},
			want:     true,
		},
		{
			name:     "all filters must match — one wrong value fails",
			filters:  map[string]string{"env": "prod", "tier": "enterprise"},
			metadata: map[string]string{"env": "prod", "tier": "free"},
			want:     false,
		},
		{
			name:     "all filters must match — one missing key fails",
			filters:  map[string]string{"env": "prod", "tier": "enterprise"},
			metadata: map[string]string{"env": "prod"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesFilters(tt.filters, tt.metadata)
			if got != tt.want {
				t.Errorf("matchesFilters(%v, %v) = %v, want %v", tt.filters, tt.metadata, got, tt.want)
			}
		})
	}
}

// ── TestSelectArm ─────────────────────────────────────────────────────────────

func TestSelectArm(t *testing.T) {
	t.Run("nil arms returns nil", func(t *testing.T) {
		if got := selectArm(nil, nil, nil); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("all-zero weights returns nil", func(t *testing.T) {
		arms := []models.LocalExperimentArm{
			{ID: uuid.New(), Weight: 0},
			{ID: uuid.New(), Weight: 0},
		}
		if got := selectArm(arms, nil, nil); got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("single arm always selected regardless of random", func(t *testing.T) {
		armID := uuid.New()
		arms := []models.LocalExperimentArm{{ID: armID, Model: "gpt-4o", Weight: 100}}
		for range 20 {
			got := selectArm(arms, nil, nil)
			if got == nil {
				t.Fatal("expected arm, got nil")
			}
			if got.ID != armID {
				t.Errorf("expected arm %v, got %v", armID, got.ID)
			}
		}
	})

	t.Run("sticky key is deterministic for the same metadata value", func(t *testing.T) {
		key := strPtr("user_id")
		arms := twoArms()
		meta := map[string]string{"user_id": "user-abc-123"}

		first := selectArm(arms, key, meta)
		if first == nil {
			t.Fatal("expected arm, got nil")
		}
		for range 50 {
			got := selectArm(arms, key, meta)
			if got == nil {
				t.Fatal("expected arm, got nil")
			}
			if got.ID != first.ID {
				t.Errorf("sticky arm selection is not deterministic: got %v, want %v", got.ID, first.ID)
			}
		}
	})

	t.Run("sticky key with distinct values distributes across arms", func(t *testing.T) {
		key := strPtr("user_id")
		arms := twoArms()
		seen := map[uuid.UUID]bool{}
		users := []string{"alice", "bob", "charlie", "dave", "eve", "frank", "grace", "henry", "iris", "jack"}
		for _, u := range users {
			got := selectArm(arms, key, map[string]string{"user_id": u})
			if got != nil {
				seen[got.ID] = true
			}
		}
		if len(seen) < 2 {
			t.Error("expected sticky hash to distribute across both arms with 10 distinct user values")
		}
	})

	t.Run("sticky key with missing metadata value falls back to random — no panic", func(t *testing.T) {
		key := strPtr("user_id")
		arms := twoArms()
		meta := map[string]string{} // sticky key absent
		for range 20 {
			got := selectArm(arms, key, meta)
			if got == nil {
				t.Error("expected arm, got nil")
			}
		}
	})

	t.Run("no sticky key uses random selection — no panic", func(t *testing.T) {
		arms := twoArms()
		for range 20 {
			got := selectArm(arms, nil, map[string]string{"user_id": "alice"})
			if got == nil {
				t.Error("expected arm, got nil")
			}
		}
	})
}

// ── TestOverrideModel ─────────────────────────────────────────────────────────

func TestOverrideModel(t *testing.T) {
	t.Run("replaces model field in valid JSON body", func(t *testing.T) {
		body := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}],"temperature":0.7}`)
		got, err := OverrideModel(body, "claude-3-5-sonnet-20241022")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var result map[string]json.RawMessage
		if err := json.Unmarshal(got, &result); err != nil {
			t.Fatalf("result is not valid JSON: %v", err)
		}
		var model string
		if err := json.Unmarshal(result["model"], &model); err != nil {
			t.Fatalf("cannot unmarshal model field: %v", err)
		}
		if model != "claude-3-5-sonnet-20241022" {
			t.Errorf("model = %q, want %q", model, "claude-3-5-sonnet-20241022")
		}
		// Other fields preserved
		if _, ok := result["messages"]; !ok {
			t.Error("messages field was dropped")
		}
		if _, ok := result["temperature"]; !ok {
			t.Error("temperature field was dropped")
		}
	})

	t.Run("adds model field when absent from body", func(t *testing.T) {
		body := []byte(`{"messages":[]}`)
		got, err := OverrideModel(body, "gpt-4o")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var result map[string]json.RawMessage
		if err := json.Unmarshal(got, &result); err != nil {
			t.Fatalf("result is not valid JSON: %v", err)
		}
		var model string
		if err := json.Unmarshal(result["model"], &model); err != nil {
			t.Fatalf("cannot unmarshal model field: %v", err)
		}
		if model != "gpt-4o" {
			t.Errorf("model = %q, want %q", model, "gpt-4o")
		}
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		_, err := OverrideModel([]byte(`not json`), "gpt-4o")
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("returns error for JSON array (not an object)", func(t *testing.T) {
		_, err := OverrideModel([]byte(`["model","gpt-4o"]`), "gpt-4o")
		if err == nil {
			t.Error("expected error for JSON array, got nil")
		}
	})

	t.Run("returns error for empty body", func(t *testing.T) {
		_, err := OverrideModel([]byte(``), "gpt-4o")
		if err == nil {
			t.Error("expected error for empty body, got nil")
		}
	})
}

// ── TestRoute ─────────────────────────────────────────────────────────────────

func TestRoute(t *testing.T) {
	orgID := uuid.New()
	apiKeyID := uuid.New()
	now := time.Now()

	newRouter := func(experiments []models.LocalExperiment) *ExperimentRouter {
		r := NewExperimentRouter(&mockExperimentStore{experiments: experiments})
		r.cacheTTL = 0 // disable cache so each call hits the store
		return r
	}

	t.Run("no experiments returns nil", func(t *testing.T) {
		r := newRouter(nil)
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})

	t.Run("inactive experiment is skipped", func(t *testing.T) {
		exp := makeActiveExperiment(nil, nil, nil, twoArms())
		exp.Status = "paused"
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got != nil {
			t.Errorf("expected nil for paused experiment, got %+v", got)
		}
	})

	t.Run("experiment with mismatched api_key_id is skipped", func(t *testing.T) {
		otherKeyID := uuid.New()
		exp := makeActiveExperiment(uuidPtr(otherKeyID), nil, nil, twoArms())
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got != nil {
			t.Errorf("expected nil for wrong api_key_id, got %+v", got)
		}
	})

	t.Run("experiment with nil api_key_id matches any key", func(t *testing.T) {
		exp := makeActiveExperiment(nil, nil, nil, twoArms())
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "original-model", now)
		if got == nil {
			t.Fatal("expected assignment, got nil")
		}
		if got.ExperimentID != exp.ID {
			t.Errorf("experiment_id = %v, want %v", got.ExperimentID, exp.ID)
		}
		if got.OriginalModel != "original-model" {
			t.Errorf("original_model = %q, want %q", got.OriginalModel, "original-model")
		}
	})

	t.Run("experiment with matching api_key_id returns assignment", func(t *testing.T) {
		exp := makeActiveExperiment(uuidPtr(apiKeyID), nil, nil, twoArms())
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got == nil {
			t.Fatal("expected assignment, got nil")
		}
		if got.ExperimentID != exp.ID {
			t.Errorf("experiment_id = %v, want %v", got.ExperimentID, exp.ID)
		}
	})

	t.Run("metadata filter mismatch skips experiment", func(t *testing.T) {
		exp := makeActiveExperiment(nil, map[string]string{"tier": "enterprise"}, nil, twoArms())
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{"tier": "free"}, "", now)
		if got != nil {
			t.Errorf("expected nil for metadata mismatch, got %+v", got)
		}
	})

	t.Run("metadata filter match returns assignment", func(t *testing.T) {
		exp := makeActiveExperiment(nil, map[string]string{"tier": "enterprise"}, nil, twoArms())
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{"tier": "enterprise", "user": "alice"}, "", now)
		if got == nil {
			t.Fatal("expected assignment, got nil")
		}
	})

	t.Run("experiment with no arms is skipped", func(t *testing.T) {
		exp := makeActiveExperiment(nil, nil, nil, nil)
		r := newRouter([]models.LocalExperiment{exp})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got != nil {
			t.Errorf("expected nil for armless experiment, got %+v", got)
		}
	})

	t.Run("assigned model comes from the selected arm", func(t *testing.T) {
		stickyKey := strPtr("user_id")
		arms := twoArms()
		exp := makeActiveExperiment(nil, nil, stickyKey, arms)
		r := newRouter([]models.LocalExperiment{exp})

		meta := map[string]string{"user_id": "fixed-user"}
		first := r.Route(context.Background(), orgID, &apiKeyID, meta, "original", now)
		if first == nil {
			t.Fatal("expected assignment, got nil")
		}
		// Verify assigned model is one of the arm models
		validModels := map[string]bool{arms[0].Model: true, arms[1].Model: true}
		if !validModels[first.AssignedModel] {
			t.Errorf("assigned model %q is not one of the experiment arm models", first.AssignedModel)
		}
		// Sticky: same metadata always produces same result
		for range 20 {
			got := r.Route(context.Background(), orgID, &apiKeyID, meta, "original", now)
			if got == nil {
				t.Fatal("expected assignment, got nil")
			}
			if got.AssignedModel != first.AssignedModel {
				t.Errorf("sticky routing not deterministic: got %q, want %q", got.AssignedModel, first.AssignedModel)
			}
		}
	})

	t.Run("first matching experiment wins", func(t *testing.T) {
		expA := makeActiveExperiment(nil, nil, nil, twoArms())
		expB := makeActiveExperiment(nil, nil, nil, twoArms())
		r := newRouter([]models.LocalExperiment{expA, expB})
		got := r.Route(context.Background(), orgID, &apiKeyID, map[string]string{}, "", now)
		if got == nil {
			t.Fatal("expected assignment, got nil")
		}
		if got.ExperimentID != expA.ID {
			t.Errorf("expected first experiment %v to win, got %v", expA.ID, got.ExperimentID)
		}
	})
}

// ── TestGetExperiments_Cache ──────────────────────────────────────────────────

func TestGetExperiments_Cache(t *testing.T) {
	t.Run("stale cache returned on store error", func(t *testing.T) {
		store := &mockExperimentStore{
			experiments: []models.LocalExperiment{makeActiveExperiment(nil, nil, nil, twoArms())},
		}
		r := NewExperimentRouter(store)
		r.cacheTTL = time.Hour // long TTL so cache doesn't expire during test

		orgID := uuid.New()
		ctx := context.Background()

		// First call populates the cache
		first := r.getExperiments(ctx, orgID, time.Now())
		if len(first) == 0 {
			t.Fatal("expected experiments from store, got none")
		}

		// Store now returns an error
		store.experiments = nil
		store.err = errors.New("db connection lost")

		// Force cache expiry by temporarily zeroing the TTL
		r.cacheTTL = 0
		stale := r.getExperiments(ctx, orgID, time.Now())
		if len(stale) == 0 {
			t.Error("expected stale cache to be returned on store error, got none")
		}
	})

	t.Run("nil returned when no cache and store errors", func(t *testing.T) {
		store := &mockExperimentStore{err: errors.New("db down")}
		r := NewExperimentRouter(store)
		r.cacheTTL = 0

		got := r.getExperiments(context.Background(), uuid.New(), time.Now())
		if got != nil {
			t.Errorf("expected nil with no cache and store error, got %v", got)
		}
	})
}
