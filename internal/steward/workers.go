package steward

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"

	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/proxy"
	"github.com/superset-studio/majordomo-steward/internal/repositories"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
)

// resultsReporterStore lets the ResultsReporter use both repositories as a
// single store via embedded method promotion.
type resultsReporterStore struct {
	*repositories.ReplayRepository
	*repositories.EvalRepository
}

// orgWorkers holds the background workers for a single registered org. The
// per-entity syncers (keysync, cloudsync, providerkeysync, experimentsync) and
// the replay/eval executor are all driven by a single WorkTicker; only the
// reporters retain their own push loops.
type orgWorkers struct {
	reporter        *stewardclient.Reporter
	resultsReporter *stewardclient.ResultsReporter
	workTicker      *stewardclient.WorkTicker
}

// WorkerManager manages per-org background workers. Workers are started at
// server startup (LoadFromDB) or dynamically at runtime (StartOrg) when the
// managed steward claims new org assignments.
type WorkerManager struct {
	cfg             *config.Config
	store           *repositories.StewardRepository
	cloudRepo       *repositories.CloudStorageRepository
	experimentRepo  *repositories.ExperimentRepository
	replayRepo      *repositories.ReplayRepository
	evalRepo        *repositories.EvalRepository
	providerKeyRepo *repositories.ProviderKeyRepository
	proxy           *proxy.Handler
	secrets         secrets.SecretStore

	mu     sync.Mutex
	active map[uuid.UUID]*orgWorkers
}

// NewWorkerManager creates a WorkerManager.
func NewWorkerManager(
	cfg *config.Config,
	store *repositories.StewardRepository,
	cloudRepo *repositories.CloudStorageRepository,
	experimentRepo *repositories.ExperimentRepository,
	replayRepo *repositories.ReplayRepository,
	evalRepo *repositories.EvalRepository,
	providerKeyRepo *repositories.ProviderKeyRepository,
	proxyHandler *proxy.Handler,
	secretStore secrets.SecretStore,
) *WorkerManager {
	return &WorkerManager{
		cfg:             cfg,
		store:           store,
		cloudRepo:       cloudRepo,
		experimentRepo:  experimentRepo,
		replayRepo:      replayRepo,
		evalRepo:        evalRepo,
		providerKeyRepo: providerKeyRepo,
		proxy:           proxyHandler,
		secrets:         secretStore,
		active:          make(map[uuid.UUID]*orgWorkers),
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
//
// Layout: one Reporter and one ResultsReporter push usage to Butler on their
// own batch intervals; a single WorkTicker pulls all sync notifications
// (api keys, cloud storage, provider keys, experiments) and replay/eval jobs
// from Butler via the /work endpoint and dispatches them.
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

	resultsReporter := stewardclient.NewResultsReporter(orgCfg, resultsReporterStore{m.replayRepo, m.evalRepo})
	resultsReporter.Start()

	// Stateless syncers — WorkTicker invokes their Sync(ctx) method when the
	// matching sync_* job is received from Butler.
	keySync := stewardclient.NewKeySyncer(orgCfg, m.store)
	cloudSync := stewardclient.NewCloudStorageSyncer(orgCfg, m.cloudRepo, m.secrets)
	experimentSync := stewardclient.NewExperimentSyncer(orgCfg, m.experimentRepo)
	providerKeySync := stewardclient.NewProviderKeySyncer(orgCfg, m.providerKeyRepo, m.secrets)

	replaySyncClient := stewardclient.NewReplaySyncClient(orgCfg)
	evalSyncClient := stewardclient.NewEvalSyncClient(orgCfg)
	exec := &jobExecutor{
		proxy:      m.proxy,
		store:      m.store,
		replayRepo: m.replayRepo,
		evalRepo:   m.evalRepo,
		replaySync: replaySyncClient,
		evalSync:   evalSyncClient,
	}

	workTicker := stewardclient.NewWorkTicker(
		orgCfg, keySync, cloudSync, providerKeySync, experimentSync, exec,
	)
	workTicker.Start()

	m.active[orgID] = &orgWorkers{
		reporter:        reporter,
		resultsReporter: resultsReporter,
		workTicker:      workTicker,
	}

	slog.Info("started workers for org", "org_id", orgID, "butler_url", butlerURL)
	return nil
}

// StopOrg stops the background workers for a single org and removes it from the
// active set. It is a no-op if no workers are running for the given org.
func (m *WorkerManager) StopOrg(ctx context.Context, orgID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, ok := m.active[orgID]
	if !ok {
		return
	}

	w.reporter.Stop()
	w.resultsReporter.Stop()
	w.workTicker.Stop()
	delete(m.active, orgID)
	slog.Info("stopped workers for org", "org_id", orgID)
}

// StopAll stops all active per-org workers. It blocks until all workers finish.
func (m *WorkerManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for orgID, w := range m.active {
		w.reporter.Stop()
		w.resultsReporter.Stop()
		w.workTicker.Stop()
		slog.Info("stopped workers for org", "org_id", orgID)
	}
	m.active = make(map[uuid.UUID]*orgWorkers)
}
