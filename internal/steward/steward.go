// Package steward wires all Steward dependencies and returns a running Server.
package steward

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/api"
	"github.com/superset-studio/majordomo-steward/internal/auth"
	"github.com/superset-studio/majordomo-steward/internal/claudecode"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/migrate"
	"github.com/superset-studio/majordomo-steward/internal/pricing"
	"github.com/superset-studio/majordomo-steward/internal/proxy"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/server"
	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
	"github.com/superset-studio/majordomo-steward/internal/storage"
)

// jobExecutor wraps the proxy handler to satisfy the stewardclient.JobExecutor
// interface. Replay/eval execution logic will be added here once the proxy
// package has been copied and extended.
type jobExecutor struct {
	proxy *proxy.Handler
	store *storage.PostgresStorage
}

func (e *jobExecutor) ExecuteReplay(ctx context.Context, orgID, runID uuid.UUID) error {
	slog.Warn("replay execution not yet implemented", "org_id", orgID, "run_id", runID)
	return nil
}

func (e *jobExecutor) ExecuteEval(ctx context.Context, orgID, runID uuid.UUID) error {
	slog.Warn("eval execution not yet implemented", "org_id", orgID, "run_id", runID)
	return nil
}

// Server is a fully initialised Steward ready to call Start() on.
type Server struct {
	inner         *server.Server
	store         *storage.PostgresStorage
	pricing       *pricing.Service
	workers       *WorkerManager
	managedPoller *stewardclient.ManagedOrgPoller
}

// Start begins listening for HTTP requests. It blocks until the server stops.
func (s *Server) Start() error {
	return s.inner.Start()
}

// ShutdownWithTimeout gracefully stops the server, drains all reporter queues,
// then closes the store and pricing service.
func (s *Server) ShutdownWithTimeout(timeout time.Duration) error {
	err := s.inner.ShutdownWithTimeout(timeout)

	if s.managedPoller != nil {
		s.managedPoller.Stop()
	}
	if s.workers != nil {
		s.workers.StopAll()
	}

	s.pricing.Close()
	s.store.Close()

	return err
}

// Build initialises all Steward dependencies and returns a Server.
func Build(ctx context.Context, cfg *config.Config) (*Server, error) {
	// ── Database ──────────────────────────────────────────────────────────────
	store, err := storage.NewPostgresStorage(ctx, cfg.Storage.Postgres.DSN(), cfg.Storage.Postgres.MaxConns, &storage.PostgresStorageConfig{
		HLLFlushInterval:   cfg.Metadata.HLLFlushInterval,
		ActiveKeysCacheTTL: cfg.Metadata.ActiveKeysCacheTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	// ── Migrations ────────────────────────────────────────────────────────────
	if err := migrate.Run(store.DB(), "./migrations"); err != nil {
		store.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}
	slog.Info("migrations applied")

	// ── Pricing ───────────────────────────────────────────────────────────────
	pricingSvc := pricing.NewService(
		cfg.Pricing.RemoteURL,
		cfg.Pricing.FallbackFile,
		cfg.Pricing.AliasesFile,
		cfg.Pricing.RefreshInterval,
	)

	// ── Auth ──────────────────────────────────────────────────────────────────
	resolver := auth.NewResolver(store)

	var proxyResolver *auth.ProxyResolver
	if cfg.Secrets.EncryptionKey != "" {
		secretStore, err := secrets.NewAESStore(cfg.Secrets.EncryptionKey)
		if err != nil {
			store.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("initialise secret store: %w", err)
		}
		proxyResolver = auth.NewProxyResolver(store, secretStore)
		slog.Info("proxy key support enabled")
	}

	// ── Claude Code Session Manager ───────────────────────────────────────────
	sessionMgr := claudecode.NewSessionManager(store)

	// ── Proxy Handler ─────────────────────────────────────────────────────────
	userBodyStorage := storage.NewUserBodyStorage()

	var proxySecretStore secrets.SecretStore
	if cfg.Secrets.EncryptionKey != "" {
		proxySecretStore, _ = secrets.NewAESStore(cfg.Secrets.EncryptionKey)
	}

	proxyHandler := proxy.NewHandler(
		store, userBodyStorage, store,
		proxySecretStore, pricingSvc, resolver, proxyResolver, sessionMgr, cfg,
	)

	// ── Worker Manager (per-org background workers) ───────────────────────────
	var workerMgr *WorkerManager
	if proxySecretStore != nil {
		workerMgr = NewWorkerManager(cfg, store, proxyHandler, proxySecretStore)
		if err := workerMgr.LoadFromDB(ctx); err != nil {
			store.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("load registered orgs: %w", err)
		}
	}

	// ── Managed Org Poller (only for Majordomo-hosted steward instances) ──────
	var managedPoller *stewardclient.ManagedOrgPoller
	if cfg.Managed.Enabled && cfg.Managed.MasterToken != "" && cfg.Managed.ButlerURL != "" {
		if workerMgr == nil {
			store.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("managed mode requires ENCRYPTION_KEY to be set")
		}
		managedPoller = stewardclient.NewManagedOrgPoller(
			cfg.Managed.ButlerURL,
			cfg.Managed.MasterToken,
			cfg.Upstream,
			workerMgr,
			store,
			proxySecretStore,
		)
		managedPoller.Start()
		slog.Info("managed org poller started", "butler_url", cfg.Managed.ButlerURL)
	}

	// ── Admin Handler (local org management API) ──────────────────────────────
	var adminHandler *api.AdminHandler
	if cfg.Secrets.AdminToken != "" && workerMgr != nil && proxySecretStore != nil {
		adminHandler = api.NewAdminHandler(store, workerMgr, proxySecretStore)
		slog.Info("admin API enabled")
	}

	// ── HTTP Server ───────────────────────────────────────────────────────────
	apiHandler := api.NewHandler(store, proxySecretStore)
	claudeHandler := api.NewClaudeSessionHandler(sessionMgr, store)
	srv := server.New(&cfg.Server, proxyHandler, store, resolver, apiHandler, claudeHandler,
		cfg.Secrets.AdminToken, adminHandler)

	return &Server{
		inner:         srv,
		store:         store,
		pricing:       pricingSvc,
		workers:       workerMgr,
		managedPoller: managedPoller,
	}, nil
}
