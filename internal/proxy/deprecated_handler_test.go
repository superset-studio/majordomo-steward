package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/auth"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/deprecated"
	"github.com/superset-studio/majordomo-steward/internal/models"
	"github.com/superset-studio/majordomo-steward/internal/pricing"
)

// ── mocks ─────────────────────────────────────────────────────────────────────

// mockDeprecatedKeyStorage implements repositories.APIKeyStorage for the auth
// resolver. Only GetAPIKeyByHash and UpdateAPIKeyLastUsed are exercised by
// resolver.ResolveAPIKey; the rest panic to catch unexpected calls.
type mockDeprecatedKeyStorage struct {
	key *models.APIKey
}

func (m *mockDeprecatedKeyStorage) GetAPIKeyByHash(_ context.Context, _ string) (*models.APIKey, error) {
	return m.key, nil
}
func (m *mockDeprecatedKeyStorage) UpdateAPIKeyLastUsed(_ context.Context, _ uuid.UUID) error {
	return nil
}
func (m *mockDeprecatedKeyStorage) CreateAPIKey(_ context.Context, _ string, _ *models.CreateAPIKeyInput) (*models.APIKey, error) {
	panic("unexpected call to CreateAPIKey")
}
func (m *mockDeprecatedKeyStorage) GetAPIKeyByID(_ context.Context, _ uuid.UUID) (*models.APIKey, error) {
	panic("unexpected call to GetAPIKeyByID")
}
func (m *mockDeprecatedKeyStorage) ListAPIKeys(_ context.Context) ([]*models.APIKey, error) {
	panic("unexpected call to ListAPIKeys")
}
func (m *mockDeprecatedKeyStorage) UpdateAPIKey(_ context.Context, _ uuid.UUID, _ *models.UpdateAPIKeyInput) (*models.APIKey, error) {
	panic("unexpected call to UpdateAPIKey")
}
func (m *mockDeprecatedKeyStorage) RevokeAPIKey(_ context.Context, _ uuid.UUID) error {
	panic("unexpected call to RevokeAPIKey")
}
func (m *mockDeprecatedKeyStorage) ListAPIKeysByUserID(_ context.Context, _ uuid.UUID) ([]*models.APIKey, error) {
	panic("unexpected call to ListAPIKeysByUserID")
}
func (m *mockDeprecatedKeyStorage) ListAPIKeysByOrgID(_ context.Context, _ uuid.UUID) ([]*models.APIKey, error) {
	panic("unexpected call to ListAPIKeysByOrgID")
}

// noopLogWriter satisfies RequestLogWriter without doing anything.
type noopLogWriter struct{}

func (n *noopLogWriter) WriteRequestLog(_ context.Context, _ *models.RequestLog) {}

// ── helpers ───────────────────────────────────────────────────────────────────

// deprecatedModelJSON is a minimal deprecated-models mapping used across tests.
// gpt-3.5-turbo → gpt-4o-mini is the canonical test pair.
const deprecatedModelJSON = `{
	"_comment": "test only",
	"gpt-3.5-turbo": "gpt-4o-mini"
}`

// newDeprecatedService builds a deprecated.Service from an in-memory JSON map
// written to a temp file, so tests don't depend on files on disk.
func newDeprecatedService(t *testing.T, jsonContent string) *deprecated.Service {
	t.Helper()
	f, err := os.CreateTemp("", "deprecated-*.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString(jsonContent); err != nil {
		t.Fatal(err)
	}
	f.Close()
	return deprecated.NewService(f.Name())
}

// newTestHandlerWithDeprecated wires up a minimal proxy Handler backed by:
//   - a fake upstream whose URL is fakeUpstreamURL
//   - the given deprecated.Service
//   - an API key whose DeprecatedModelBehavior is set to behavior
//
// The pricing service has no pricing data (returns 0 cost), which is fine for
// these tests. The returned Handler is ready to serve via ServeHTTP.
func newTestHandlerWithDeprecated(
	t *testing.T,
	fakeUpstreamURL string,
	deprecatedSvc *deprecated.Service,
	behavior models.DeprecatedModelBehavior,
) *Handler {
	t.Helper()

	apiKey := &models.APIKey{
		ID:                      uuid.New(),
		KeyHash:                 auth.HashAPIKey("test-api-key"),
		Name:                    "test-key",
		IsActive:                true,
		DeprecatedModelBehavior: behavior,
	}

	resolver := auth.NewResolver(&mockDeprecatedKeyStorage{key: apiKey})

	// Pricing service with no remote URL or local files — returns zero cost.
	// The background refresh goroutine is idle (24h ticker) and acceptable to leak.
	pricingSvc := pricing.NewService("", "", "", 24*time.Hour)

	cfg := &config.Config{
		Server: config.ServerConfig{
			UpstreamTimeout:     10 * time.Second,
			StreamHeaderTimeout: 5 * time.Second,
		},
		Providers: config.ProvidersConfig{
			OpenAI: config.ProviderConfig{BaseURL: fakeUpstreamURL},
		},
	}

	return NewHandler(
		&noopLogWriter{},
		nil, // userBodyStorage — nil-safe
		nil, // cloudStorageStore — nil-safe
		nil, // secretStore — nil-safe
		pricingSvc,
		deprecatedSvc,
		resolver,
		nil, // proxyResolver — nil-safe
		nil, // sessionMgr — nil-safe
		cfg,
	)
}

// openAIRequest serialises a minimal chat-completions request with the given model.
func openAIRequest(model string) []byte {
	b, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": "hello"},
		},
	})
	return b
}

// openAIResponse is a minimal valid chat-completions JSON response. The model
// field here is what the fake upstream *claims* to have used.
func openAIResponse(model string) []byte {
	b, _ := json.Marshal(map[string]any{
		"id":     "chatcmpl-test",
		"object": "chat.completion",
		"model":  model,
		"choices": []map[string]any{
			{"index": 0, "message": map[string]string{"role": "assistant", "content": "ok"}, "finish_reason": "stop"},
		},
		"usage": map[string]int{
			"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
		},
	})
	return b
}

// extractModelFromBody parses the "model" field from a JSON request body.
func extractModelFromBody(t *testing.T, body []byte) string {
	t.Helper()
	var req map[string]json.RawMessage
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("could not unmarshal upstream request body: %v\nbody: %s", err, body)
	}
	var model string
	if err := json.Unmarshal(req["model"], &model); err != nil {
		t.Fatalf("could not unmarshal model field: %v", err)
	}
	return model
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestDeprecatedModel_Passthrough(t *testing.T) {
	var capturedBody []byte
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-3.5-turbo"))
	}))
	defer fake.Close()

	deprecatedSvc := newDeprecatedService(t, deprecatedModelJSON)
	h := newTestHandlerWithDeprecated(t, fake.URL, deprecatedSvc, models.DeprecatedModelBehaviorPassthrough)

	reqBody := openAIRequest("gpt-3.5-turbo")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("X-Majordomo-Key", "test-api-key")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", rr.Code, rr.Body.String())
	}

	// Upstream must receive the original deprecated model unchanged.
	if got := extractModelFromBody(t, capturedBody); got != "gpt-3.5-turbo" {
		t.Errorf("upstream received model %q, want %q (passthrough should not redirect)", got, "gpt-3.5-turbo")
	}

	// No warning headers on passthrough.
	if h := rr.Header().Get("X-Majordomo-Deprecated-Model"); h != "" {
		t.Errorf("expected no X-Majordomo-Deprecated-Model header, got %q", h)
	}
	if h := rr.Header().Get("X-Majordomo-Deprecated-Replacement"); h != "" {
		t.Errorf("expected no X-Majordomo-Deprecated-Replacement header, got %q", h)
	}
}

func TestDeprecatedModel_Redirect(t *testing.T) {
	var capturedBody []byte
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-4o-mini"))
	}))
	defer fake.Close()

	deprecatedSvc := newDeprecatedService(t, deprecatedModelJSON)
	h := newTestHandlerWithDeprecated(t, fake.URL, deprecatedSvc, models.DeprecatedModelBehaviorRedirect)

	reqBody := openAIRequest("gpt-3.5-turbo")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("X-Majordomo-Key", "test-api-key")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", rr.Code, rr.Body.String())
	}

	// Upstream must receive the replacement model.
	if got := extractModelFromBody(t, capturedBody); got != "gpt-4o-mini" {
		t.Errorf("upstream received model %q, want %q (redirect should substitute replacement)", got, "gpt-4o-mini")
	}

	// Redirect must NOT set warning headers.
	if h := rr.Header().Get("X-Majordomo-Deprecated-Model"); h != "" {
		t.Errorf("expected no X-Majordomo-Deprecated-Model header on redirect, got %q", h)
	}
	if h := rr.Header().Get("X-Majordomo-Deprecated-Replacement"); h != "" {
		t.Errorf("expected no X-Majordomo-Deprecated-Replacement header on redirect, got %q", h)
	}
}

func TestDeprecatedModel_Warn(t *testing.T) {
	var capturedBody []byte
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-4o-mini"))
	}))
	defer fake.Close()

	deprecatedSvc := newDeprecatedService(t, deprecatedModelJSON)
	h := newTestHandlerWithDeprecated(t, fake.URL, deprecatedSvc, models.DeprecatedModelBehaviorWarn)

	reqBody := openAIRequest("gpt-3.5-turbo")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("X-Majordomo-Key", "test-api-key")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", rr.Code, rr.Body.String())
	}

	// Upstream must receive the replacement model.
	if got := extractModelFromBody(t, capturedBody); got != "gpt-4o-mini" {
		t.Errorf("upstream received model %q, want %q (warn should substitute replacement)", got, "gpt-4o-mini")
	}

	// Warn must set both deprecation headers on the response.
	if got := rr.Header().Get("X-Majordomo-Deprecated-Model"); got != "gpt-3.5-turbo" {
		t.Errorf("X-Majordomo-Deprecated-Model = %q, want %q", got, "gpt-3.5-turbo")
	}
	if got := rr.Header().Get("X-Majordomo-Deprecated-Replacement"); got != "gpt-4o-mini" {
		t.Errorf("X-Majordomo-Deprecated-Replacement = %q, want %q", got, "gpt-4o-mini")
	}
}

func TestDeprecatedModel_NonDeprecated_Unchanged(t *testing.T) {
	var capturedBody []byte
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-4o"))
	}))
	defer fake.Close()

	deprecatedSvc := newDeprecatedService(t, deprecatedModelJSON)

	// Try each behavior — a non-deprecated model must pass through unchanged regardless.
	behaviors := []models.DeprecatedModelBehavior{
		models.DeprecatedModelBehaviorPassthrough,
		models.DeprecatedModelBehaviorRedirect,
		models.DeprecatedModelBehaviorWarn,
	}
	for _, behavior := range behaviors {
		t.Run(string(behavior), func(t *testing.T) {
			h := newTestHandlerWithDeprecated(t, fake.URL, deprecatedSvc, behavior)

			reqBody := openAIRequest("gpt-4o") // not in deprecated map
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
			req.Header.Set("X-Majordomo-Key", "test-api-key")
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d — body: %s", rr.Code, rr.Body.String())
			}

			// Upstream must receive the original model.
			if got := extractModelFromBody(t, capturedBody); got != "gpt-4o" {
				t.Errorf("upstream received model %q, want %q (non-deprecated should be unchanged)", got, "gpt-4o")
			}

			// No warning headers regardless of behavior.
			if h := rr.Header().Get("X-Majordomo-Deprecated-Model"); h != "" {
				t.Errorf("unexpected X-Majordomo-Deprecated-Model header: %q", h)
			}
		})
	}
}

func TestDeprecatedModel_NilService_Passthrough(t *testing.T) {
	var capturedBody []byte
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-3.5-turbo"))
	}))
	defer fake.Close()

	// When no deprecated service is configured, requests pass through unchanged.
	h := newTestHandlerWithDeprecated(t, fake.URL, nil, models.DeprecatedModelBehaviorRedirect)

	reqBody := openAIRequest("gpt-3.5-turbo")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("X-Majordomo-Key", "test-api-key")
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d — body: %s", rr.Code, rr.Body.String())
	}

	// Even with redirect behavior set on the key, nil service means no substitution.
	if got := extractModelFromBody(t, capturedBody); got != "gpt-3.5-turbo" {
		t.Errorf("upstream received model %q, want %q (nil deprecated service should not redirect)", got, "gpt-3.5-turbo")
	}

	if h := rr.Header().Get("X-Majordomo-Deprecated-Model"); h != "" {
		t.Errorf("unexpected deprecation header with nil service: %q", h)
	}
}

// TestDeprecatedModel_WarnHeaders_BeforeBody verifies that warning headers are
// visible in the response even when the upstream response is non-SSE (buffered
// path). Headers set on w.Header() before the first Write are included in the
// response regardless of whether WriteHeader was called explicitly.
func TestDeprecatedModel_WarnHeaders_BeforeBody(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(openAIResponse("gpt-4o-mini"))
	}))
	defer fake.Close()

	deprecatedSvc := newDeprecatedService(t, deprecatedModelJSON)
	h := newTestHandlerWithDeprecated(t, fake.URL, deprecatedSvc, models.DeprecatedModelBehaviorWarn)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader(openAIRequest("gpt-3.5-turbo")))
	req.Header.Set("X-Majordomo-Key", "test-api-key")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// Body must contain the upstream response.
	if !strings.Contains(rr.Body.String(), "gpt-4o-mini") {
		t.Errorf("response body should contain upstream model, got: %s", rr.Body.String())
	}

	// Headers must be present (they were set on w.Header() before WriteHeader).
	if got := rr.Header().Get("X-Majordomo-Deprecated-Model"); got != "gpt-3.5-turbo" {
		t.Errorf("X-Majordomo-Deprecated-Model = %q, want %q", got, "gpt-3.5-turbo")
	}
}
