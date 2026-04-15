package steward

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/proxy"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
	"github.com/superset-studio/majordomo-steward/internal/storage"
)

// orgWorkers holds the three background workers for a single registered org.
type orgWorkers struct {
	reporter *stewardclient.Reporter
	keysync  *stewardclient.KeySyncer
	jobs     *stewardclient.JobPoller
}

// WorkerManager manages per-org background workers (reporter, key-syncer, job-poller).
// Workers can be started at server startup (LoadFromDB) or dynamically at runtime
// (StartOrg) when the managed steward claims new org assignments.
type WorkerManager struct {
	cfg     *config.Config
	store   *storage.PostgresStorage
	proxy   *proxy.Handler
	secrets secrets.SecretStore

	mu     sync.Mutex
	active map[uuid.UUID]*orgWorkers
}

// NewWorkerManager creates a WorkerManager. Call LoadFromDB to start workers for
// all already-registered orgs, or StartOrg to add individual orgs at runtime.
func NewWorkerManager(
	cfg *config.Config,
	store *storage.PostgresStorage,
	proxyHandler *proxy.Handler,
	secretStore secrets.SecretStore,
) *WorkerManager {
	return &WorkerManager{
		cfg:     cfg,
		store:   store,
		proxy:   proxyHandler,
		secrets: secretStore,
		active:  make(map[uuid.UUID]*orgWorkers),
	}
}

// LoadFromDB reads all rows from registered_orgs, decrypts their tokens, and
// calls StartOrg for each. Errors per org are logged but do not abort others.
func (m *WorkerManager) LoadFromDB(ctx context.Context) error {
	orgs, err := m.store.ListRegisteredOrgs(ctx)
	if err != nil {
		return fmt.Errorf("list registered orgs: %w", err)
	}

	for _, org := range orgs {
		plaintext, err := m.secrets.Decrypt(org.TokenEncrypted)
		if err != nil {
			slog.Error("failed to decrypt token for registered org — skipping",
				"org_id", org.OrgID, "error", err)
			continue
		}
		if err := m.StartOrg(org.OrgID, org.ButlerURL, plaintext); err != nil {
			slog.Error("failed to start workers for registered org — skipping",
				"org_id", org.OrgID, "error", err)
		}
	}

	return nil
}

// StartOrg starts background workers for one org. It is idempotent: if workers
// for the org are already running, this is a no-op.
func (m *WorkerManager) StartOrg(orgID uuid.UUID, butlerURL, plaintextToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, already := m.active[orgID]; already {
		return nil
	}

	orgCfg := m.cfg.Upstream
	orgCfg.OrgID = orgID
	orgCfg.ButlerBaseURL = butlerURL
	orgCfg.StewardToken = plaintextToken

	reporter := stewardclient.NewReporter(orgCfg, m.store)
	reporter.Start()

	keysync := stewardclient.NewKeySyncer(orgCfg, m.store)
	keysync.Start()

	exec := &jobExecutor{proxy: m.proxy, store: m.store}
	jobs := stewardclient.NewJobPoller(orgCfg, exec)
	jobs.Start()

	m.active[orgID] = &orgWorkers{
		reporter: reporter,
		keysync:  keysync,
		jobs:     jobs,
	}

	slog.Info("started workers for org", "org_id", orgID, "butler_url", butlerURL)
	return nil
}

// StopAll stops all active per-org workers. It blocks until all workers finish.
func (m *WorkerManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for orgID, w := range m.active {
		w.reporter.Stop()
		w.keysync.Stop()
		w.jobs.Stop()
		slog.Info("stopped workers for org", "org_id", orgID)
	}
	m.active = make(map[uuid.UUID]*orgWorkers)
}
