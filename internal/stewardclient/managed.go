package stewardclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
)

// OrgStarter can start per-org workers at runtime. Implemented by WorkerManager.
type OrgStarter interface {
	StartOrg(orgID uuid.UUID, butlerURL, plaintextToken string) error
}

// OrgRegistrar can persist a registered org locally. Implemented by storage.PostgresStorage.
type OrgRegistrar interface {
	RegisterOrg(ctx context.Context, orgID uuid.UUID, name, butlerURL, tokenEncrypted string) error
}

// pendingAssignment is the response body item from GET /steward/managed/pending.
type pendingAssignment struct {
	ID      uuid.UUID `json:"id"`
	OrgID   uuid.UUID `json:"org_id"`
	OrgName string    `json:"org_name"`
	Token   string    `json:"token"`
}

// ManagedOrgPoller polls butler's managed assignment queue on behalf of a
// Majordomo-hosted steward, claims new org assignments, and starts workers.
type ManagedOrgPoller struct {
	butlerURL string
	token     string
	intervals config.StewardConfig
	starter   OrgStarter
	registrar OrgRegistrar
	secrets   secrets.SecretStore
	client    *http.Client
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// NewManagedOrgPoller creates a ManagedOrgPoller. Call Start() to begin polling.
func NewManagedOrgPoller(
	butlerURL, masterToken string,
	intervals config.StewardConfig,
	starter OrgStarter,
	registrar OrgRegistrar,
	secretStore secrets.SecretStore,
) *ManagedOrgPoller {
	return &ManagedOrgPoller{
		butlerURL: strings.TrimRight(butlerURL, "/"),
		token:     masterToken,
		intervals: intervals,
		starter:   starter,
		registrar: registrar,
		secrets:   secretStore,
		client:    &http.Client{Timeout: 15 * time.Second},
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start launches the background polling goroutine. It must be called once.
func (p *ManagedOrgPoller) Start() {
	go p.run()
}

// Stop signals the poller to exit and waits until it has.
func (p *ManagedOrgPoller) Stop() {
	close(p.stopCh)
	<-p.doneCh
}

func (p *ManagedOrgPoller) run() {
	defer close(p.doneCh)

	// Poll immediately so newly-provisioned orgs are claimed without delay.
	p.poll()

	interval := p.intervals.WorkTickInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.poll()
		case <-p.stopCh:
			return
		}
	}
}

func (p *ManagedOrgPoller) poll() {
	assignments, err := p.fetchPending()
	if err != nil {
		slog.Warn("managed poller: fetch pending failed", "error", err)
		return
	}

	for _, a := range assignments {
		if err := p.handleAssignment(a); err != nil {
			slog.Warn("managed poller: handle assignment failed",
				"assignment_id", a.ID, "org_id", a.OrgID, "error", err)
		}
	}
}

func (p *ManagedOrgPoller) handleAssignment(a pendingAssignment) error {
	ctx := context.Background()

	// Encrypt the plaintext token for local storage.
	encrypted, err := p.secrets.Encrypt(a.Token)
	if err != nil {
		return fmt.Errorf("encrypt token: %w", err)
	}

	// Persist locally so the org survives a restart.
	if err := p.registrar.RegisterOrg(ctx, a.OrgID, a.OrgName, p.butlerURL, encrypted); err != nil {
		return fmt.Errorf("register org locally: %w", err)
	}

	// Start workers for this org immediately.
	if err := p.starter.StartOrg(a.OrgID, p.butlerURL, a.Token); err != nil {
		return fmt.Errorf("start org workers: %w", err)
	}

	// Claim the assignment in butler so it won't be returned again.
	if err := p.claim(a.ID); err != nil {
		// Non-fatal: the org is running. Next poll will see it already started (idempotent).
		slog.Warn("managed poller: claim assignment failed — workers already started",
			"assignment_id", a.ID, "error", err)
	}

	slog.Info("managed poller: claimed org assignment", "org_id", a.OrgID, "org_name", a.OrgName)
	return nil
}

func (p *ManagedOrgPoller) fetchPending() ([]pendingAssignment, error) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		p.butlerURL+"/api/v1/steward/managed/pending",
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var assignments []pendingAssignment
	if err := json.NewDecoder(resp.Body).Decode(&assignments); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return assignments, nil
}

func (p *ManagedOrgPoller) claim(assignmentID uuid.UUID) error {
	url := fmt.Sprintf("%s/api/v1/steward/managed/assignments/%s/claim", p.butlerURL, assignmentID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(nil))
	if err != nil {
		return fmt.Errorf("build claim request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("post claim: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("butler returned %d on claim", resp.StatusCode)
	}

	return nil
}
