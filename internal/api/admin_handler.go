package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/httputil"
	"github.com/superset-studio/majordomo-steward/internal/repositories"
	"github.com/superset-studio/majordomo-steward/internal/services"
)

// OrgManager starts and stops per-org background workers. Implemented by
// steward.WorkerManager; defined here to avoid a circular import.
type OrgManager interface {
	StartOrg(orgID uuid.UUID, butlerURL, plaintextToken string) error
	StopOrg(ctx context.Context, orgID uuid.UUID)
}

// AdminStore is the minimal storage interface required by the admin handler.
type AdminStore interface {
	ListRegisteredOrgs(ctx context.Context) ([]*repositories.RegisteredOrg, error)
	RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error
	RemoveRegisteredOrg(ctx context.Context, orgID uuid.UUID) error
	CountUnsyncedRecords(ctx context.Context, orgID uuid.UUID) (int, error)
}

// AdminHandler handles steward admin operations: listing, registering, and
// deregistering orgs via the local /admin/* API.
type AdminHandler struct {
	store    AdminStore
	workers  OrgManager
	orgRegSvc *services.OrgRegistrationService
}

// NewAdminHandler creates an AdminHandler.
func NewAdminHandler(store AdminStore, workers OrgManager, orgRegSvc *services.OrgRegistrationService) *AdminHandler {
	return &AdminHandler{
		store:     store,
		workers:   workers,
		orgRegSvc: orgRegSvc,
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

	org, err := h.orgRegSvc.RegisterOrg(r.Context(), req.ButlerURL, req.Token)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrInvalidTokenFormat):
			httputil.WriteJSONError(w, http.StatusBadRequest, "invalid token format: must start with mdm_st_")
		case errors.Is(err, services.ErrButlerValidation):
			httputil.WriteJSONError(w, http.StatusBadGateway, "failed to validate token with butler")
		default:
			slog.Error("admin: register org failed", "error", err)
			httputil.WriteJSONError(w, http.StatusInternalServerError, "failed to register org")
		}
		return
	}

	slog.Info("admin: registered org", "org_id", org.OrgID, "org_name", org.OrgName)
	httputil.WriteJSON(w, http.StatusCreated, registerOrgResponse{
		OrgID:   org.OrgID,
		OrgName: org.OrgName,
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

