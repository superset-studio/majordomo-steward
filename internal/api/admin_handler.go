package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/httputil"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/storage"
)

// OrgManager starts and stops per-org background workers. Implemented by
// steward.WorkerManager; defined here to avoid a circular import.
type OrgManager interface {
	StartOrg(orgID uuid.UUID, butlerURL, plaintextToken string) error
	StopOrg(ctx context.Context, orgID uuid.UUID)
}

// AdminStore is the minimal storage interface required by the admin handler.
type AdminStore interface {
	ListRegisteredOrgs(ctx context.Context) ([]*storage.RegisteredOrg, error)
	RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error
	RemoveRegisteredOrg(ctx context.Context, orgID uuid.UUID) error
	CountUnsyncedRecords(ctx context.Context, orgID uuid.UUID) (int, error)
}

// AdminHandler handles steward admin operations: listing, registering, and
// deregistering orgs via the local /admin/* API.
type AdminHandler struct {
	store      AdminStore
	workers    OrgManager
	secrets    secrets.SecretStore
	httpClient *http.Client
}

// NewAdminHandler creates an AdminHandler.
func NewAdminHandler(store AdminStore, workers OrgManager, secretStore secrets.SecretStore) *AdminHandler {
	return &AdminHandler{
		store:      store,
		workers:    workers,
		secrets:    secretStore,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// ── Response types ─────────────────────────────────────────────────────────────

type orgStatusResponse struct {
	OrgID        uuid.UUID `json:"org_id"`
	Name         string    `json:"name"`
	ButlerURL    string    `json:"butler_url"`
	PendingCount int       `json:"pending_count"`
	RegisteredAt time.Time `json:"registered_at"`
}

type registerOrgRequest struct {
	Token     string `json:"token"`
	ButlerURL string `json:"butler_url"`
}

type registerOrgResponse struct {
	OrgID   uuid.UUID `json:"org_id"`
	OrgName string    `json:"org_name"`
}

// meResponse mirrors the butler ingest /me response.
type meResponse struct {
	StewardID string `json:"steward_id"`
	OrgID     string `json:"org_id"`
	OrgName   string `json:"org_name"`
}

// ── Handlers ───────────────────────────────────────────────────────────────────

// HandleListOrgs handles GET /admin/orgs.
// Returns all registered orgs with their pending (unsynced) record counts.
func (h *AdminHandler) HandleListOrgs(w http.ResponseWriter, r *http.Request) {
	orgs, err := h.store.ListRegisteredOrgs(r.Context())
	if err != nil {
		slog.Error("admin: list registered orgs failed", "error", err)
		httputil.WriteJSONError(w, http.StatusInternalServerError, "failed to list orgs")
		return
	}

	resp := make([]orgStatusResponse, 0, len(orgs))
	for _, o := range orgs {
		count, err := h.store.CountUnsyncedRecords(r.Context(), o.OrgID)
		if err != nil {
			slog.Warn("admin: count unsynced records failed", "org_id", o.OrgID, "error", err)
		}
		resp = append(resp, orgStatusResponse{
			OrgID:        o.OrgID,
			Name:         o.Name,
			ButlerURL:    o.ButlerURL,
			PendingCount: count,
			RegisteredAt: o.CreatedAt,
		})
	}

	httputil.WriteJSON(w, http.StatusOK, resp)
}

// HandleRegisterOrg handles POST /admin/orgs/register.
// Validates the steward token against butler, stores it, and starts workers.
func (h *AdminHandler) HandleRegisterOrg(w http.ResponseWriter, r *http.Request) {
	var req registerOrgRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteJSONError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	if req.Token == "" || req.ButlerURL == "" {
		httputil.WriteJSONError(w, http.StatusBadRequest, "token and butler_url are required")
		return
	}
	if !strings.HasPrefix(req.Token, "mdm_st_") || len(req.Token) <= len("mdm_st_") {
		httputil.WriteJSONError(w, http.StatusBadRequest, "invalid token format: must start with mdm_st_")
		return
	}

	// Validate token and fetch org info from butler.
	me, err := h.fetchMe(r.Context(), req.ButlerURL, req.Token)
	if err != nil {
		httputil.WriteJSONError(w, http.StatusBadGateway, fmt.Sprintf("failed to validate token with butler: %v", err))
		return
	}

	orgID, err := uuid.Parse(me.OrgID)
	if err != nil {
		httputil.WriteJSONError(w, http.StatusBadGateway, "butler returned invalid org_id")
		return
	}

	encrypted, err := h.secrets.Encrypt(req.Token)
	if err != nil {
		slog.Error("admin: encrypt token failed", "error", err)
		httputil.WriteJSONError(w, http.StatusInternalServerError, "failed to store token")
		return
	}

	if err := h.store.RegisterOrg(r.Context(), orgID, me.OrgName, req.ButlerURL, encrypted); err != nil {
		slog.Error("admin: register org failed", "org_id", orgID, "error", err)
		httputil.WriteJSONError(w, http.StatusInternalServerError, "failed to register org")
		return
	}

	if err := h.workers.StartOrg(orgID, req.ButlerURL, req.Token); err != nil {
		slog.Error("admin: start org workers failed", "org_id", orgID, "error", err)
		httputil.WriteJSONError(w, http.StatusInternalServerError, "org registered but failed to start workers — restart steward to activate")
		return
	}

	slog.Info("admin: registered org", "org_id", orgID, "org_name", me.OrgName)
	httputil.WriteJSON(w, http.StatusCreated, registerOrgResponse{
		OrgID:   orgID,
		OrgName: me.OrgName,
	})
}

// HandleDeregisterOrg handles DELETE /admin/orgs/{orgID}.
// Stops workers for the org and removes it from the local registry.
func (h *AdminHandler) HandleDeregisterOrg(w http.ResponseWriter, r *http.Request) {
	orgID, err := uuid.Parse(chi.URLParam(r, "orgID"))
	if err != nil {
		httputil.WriteJSONError(w, http.StatusBadRequest, "invalid org_id")
		return
	}

	h.workers.StopOrg(r.Context(), orgID)

	if err := h.store.RemoveRegisteredOrg(r.Context(), orgID); err != nil {
		slog.Error("admin: remove registered org failed", "org_id", orgID, "error", err)
		httputil.WriteJSONError(w, http.StatusInternalServerError, "failed to deregister org")
		return
	}

	slog.Info("admin: deregistered org", "org_id", orgID)
	w.WriteHeader(http.StatusNoContent)
}

// ── Helpers ────────────────────────────────────────────────────────────────────

func (h *AdminHandler) fetchMe(ctx context.Context, butlerURL, token string) (*meResponse, error) {
	url := strings.TrimRight(butlerURL, "/") + "/api/v1/steward/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call butler: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d — check token and butler URL", resp.StatusCode)
	}

	var me meResponse
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &me, nil
}
