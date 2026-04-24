package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/secrets"
)

// Sentinel errors returned by OrgRegistrationService.
var (
	ErrInvalidTokenFormat = errors.New("invalid token format: must start with mdm_st_")
	ErrButlerValidation   = errors.New("butler token validation failed")
)

// RegisteredOrg carries the org details resolved during registration.
type RegisteredOrg struct {
	OrgID   uuid.UUID
	OrgName string
}

// OrgRegistrar is the subset of repositories.StewardRepository required by
// OrgRegistrationService. Using a narrow interface avoids a direct dependency on
// the repositories package from here.
type OrgRegistrar interface {
	RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error
}

// OrgWorkerManager starts per-org background workers after registration.
// Implemented by steward.WorkerManager.
type OrgWorkerManager interface {
	StartOrg(orgID uuid.UUID, butlerURL, plaintextToken string) error
}

// meResponse mirrors the butler /api/v1/steward/me response payload.
type meResponse struct {
	StewardID string `json:"steward_id"`
	OrgID     string `json:"org_id"`
	OrgName   string `json:"org_name"`
}

// OrgRegistrationService validates a steward token against butler, persists the
// registration, and starts per-org background workers.
type OrgRegistrationService struct {
	store      OrgRegistrar
	workers    OrgWorkerManager
	secrets    secrets.SecretStore
	httpClient *http.Client
}

// NewOrgRegistrationService constructs an OrgRegistrationService.
func NewOrgRegistrationService(
	store OrgRegistrar,
	workers OrgWorkerManager,
	secretStore secrets.SecretStore,
) *OrgRegistrationService {
	return &OrgRegistrationService{
		store:      store,
		workers:    workers,
		secrets:    secretStore,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// RegisterOrg validates the token format, calls butler's /steward/me endpoint to
// verify the token and resolve org details, encrypts and stores the registration,
// then starts the per-org workers.
func (s *OrgRegistrationService) RegisterOrg(ctx context.Context, butlerURL, token string) (*RegisteredOrg, error) {
	if !strings.HasPrefix(token, "mdm_st_") || len(token) <= len("mdm_st_") {
		return nil, ErrInvalidTokenFormat
	}

	me, err := s.fetchMe(ctx, butlerURL, token)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrButlerValidation, err)
	}

	orgID, err := uuid.Parse(me.OrgID)
	if err != nil {
		return nil, fmt.Errorf("butler returned invalid org_id: %w", err)
	}

	encrypted, err := s.secrets.Encrypt(token)
	if err != nil {
		return nil, fmt.Errorf("encrypt token: %w", err)
	}

	if err := s.store.RegisterOrg(ctx, orgID, me.OrgName, butlerURL, encrypted); err != nil {
		return nil, fmt.Errorf("store registration: %w", err)
	}

	if err := s.workers.StartOrg(orgID, butlerURL, token); err != nil {
		return nil, fmt.Errorf("start org workers: %w", err)
	}

	return &RegisteredOrg{OrgID: orgID, OrgName: me.OrgName}, nil
}

func (s *OrgRegistrationService) fetchMe(ctx context.Context, butlerURL, token string) (*meResponse, error) {
	url := strings.TrimRight(butlerURL, "/") + "/api/v1/steward/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.httpClient.Do(req)
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
