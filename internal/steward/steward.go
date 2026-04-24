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
	"github.com/superset-studio/majordomo-steward/internal/repositories"
	"github.com/superset-studio/majordomo-steward/internal/requestlog"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/server"
	"github.com/superset-studio/majordomo-steward/internal/services"
	"github.com/superset-studio/majordomo-steward/internal/stewardclient"
	"github.com/superset-studio/majordomo-steward/internal/storage"
)

// jobExecutor wraps the proxy handler to satisfy the stewardclient.JobExecutor interface.
type jobExecutor struct {
	proxy *proxy.Handler
	store *repositories.StewardRepository
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
	logWriter     *requestlog.Writer
	pricing       *pricing.Service
	workers       *WorkerManager
	managedPoller *stewardclient.ManagedOrgPoller
}

// Start begins listening for HTTP requests. It blocks until the server stops.
func (s *Server) Start() error {
	return s.inner.Start()
}

// ShutdownWithTimeout gracefully stops the server, drains all reporter queues,
// then closes the log writer and pricing service.
func (s *Server) ShutdownWithTimeout(timeout time.Duration) error {
	err := s.inner.ShutdownWithTimeout(timeout)

	if s.managedPoller != nil {
		s.managedPoller.Stop()
	}
	if s.workers != nil {
		s.workers.StopAll()
	}

	s.pricing.Close()
	s.logWriter.Close()

	return err
}

// Build initialises all Steward dependencies and returns a Server.
func Build(ctx context.Context, cfg *config.Config) (*Server, error) {
	// ── Database ──────────────────────────────────────────────────────────────
	repoCfg := repositories.DefaultConfig()
	if cfg.Metadata.HLLFlushInterval != 0 {
		repoCfg.HLLFlushInterval = cfg.Metadata.HLLFlushInterval
	}
	if cfg.Metadata.ActiveKeysCacheTTL != 0 {
		repoCfg.ActiveKeysCacheTTL = cfg.Metadata.ActiveKeysCacheTTL
	}

	db, err := repositories.Connect(ctx, cfg.Storage.Postgres.DSN(), cfg.Storage.Postgres.MaxConns)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	// ── Migrations ────────────────────────────────────────────────────────────
	if err := migrate.Run(db.DB, "./migrations"); err != nil {
		db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}
	slog.Info("migrations applied")

	// ── Infrastructure ────────────────────────────────────────────────────────
	activeKeyCache := repositories.NewActiveKeysCache(db, repoCfg.ActiveKeysCacheTTL)
	hllManager := repositories.NewHLLManager(db, repoCfg.HLLFlushInterval)

	if err := hllManager.LoadFromDB(ctx); err != nil {
		slog.Warn("failed to load HLL state from DB", "error", err)
	}
	hllManager.Start()

	// ── Repositories ──────────────────────────────────────────────────────────
	apiKeyRepo := repositories.NewAPIKeyRepository(db)
	proxyKeyRepo := repositories.NewProxyKeyRepository(db)
	claudeSessionRepo := repositories.NewClaudeSessionRepository(db)
	metadataKeyRepo := repositories.NewMetadataKeyRepository(db, activeKeyCache)
	stewardRepo := repositories.NewStewardRepository(db)
	cloudRepo := repositories.NewCloudStorageRepository(db)

	// ── Request Log Writer ────────────────────────────────────────────────────
	logWriter := requestlog.New(db, activeKeyCache, hllManager, claudeSessionRepo)

	// ── Pricing ───────────────────────────────────────────────────────────────
	pricingSvc := pricing.NewService(
		cfg.Pricing.RemoteURL,
		cfg.Pricing.FallbackFile,
		cfg.Pricing.AliasesFile,
		cfg.Pricing.RefreshInterval,
	)

	// ── Auth ──────────────────────────────────────────────────────────────────
	resolver := auth.NewResolver(apiKeyRepo)

	var proxyResolver *auth.ProxyResolver
	if cfg.Secrets.EncryptionKey != "" {
		secretStore, err := secrets.NewAESStore(cfg.Secrets.EncryptionKey)
		if err != nil {
			logWriter.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("initialise secret store: %w", err)
		}
		proxyResolver = auth.NewProxyResolver(proxyKeyRepo, secretStore)
		slog.Info("proxy key support enabled")
	}

	// ── Claude Code Session Manager ───────────────────────────────────────────
	sessionMgr := claudecode.NewSessionManager(claudeSessionRepo)

	// ── Proxy Handler ─────────────────────────────────────────────────────────
	userBodyStorage := storage.NewUserBodyStorage()

	var proxySecretStore secrets.SecretStore
	if cfg.Secrets.EncryptionKey != "" {
		proxySecretStore, _ = secrets.NewAESStore(cfg.Secrets.EncryptionKey)
	}

	proxyHandler := proxy.NewHandler(
		logWriter, userBodyStorage, cloudRepo,
		proxySecretStore, pricingSvc, resolver, proxyResolver, sessionMgr, cfg,
	)

	// ── Worker Manager (per-org background workers) ───────────────────────────
	var workerMgr *WorkerManager
	if proxySecretStore != nil {
		workerMgr = NewWorkerManager(cfg, stewardRepo, cloudRepo, proxyHandler, proxySecretStore)
		if err := workerMgr.LoadFromDB(ctx); err != nil {
			logWriter.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("load registered orgs: %w", err)
		}
	}

	// ── Managed Org Poller ────────────────────────────────────────────────────
	var managedPoller *stewardclient.ManagedOrgPoller
	if cfg.Managed.Enabled && cfg.Managed.MasterToken != "" && cfg.Managed.ButlerURL != "" {
		if workerMgr == nil {
			logWriter.Close()
			pricingSvc.Close()
			return nil, fmt.Errorf("managed mode requires ENCRYPTION_KEY to be set")
		}
		managedPoller = stewardclient.NewManagedOrgPoller(
			cfg.Managed.ButlerURL,
			cfg.Managed.MasterToken,
			cfg.Upstream,
			workerMgr,
			stewardRepo,
			proxySecretStore,
		)
		managedPoller.Start()
		slog.Info("managed org poller started", "butler_url", cfg.Managed.ButlerURL)
	}

	// ── Admin Handler ─────────────────────────────────────────────────────────
	var adminHandler *api.AdminHandler
	if cfg.Secrets.AdminToken != "" && workerMgr != nil && proxySecretStore != nil {
		orgRegSvc := services.NewOrgRegistrationService(stewardRepo, workerMgr, proxySecretStore)
		adminHandler = api.NewAdminHandler(stewardRepo, workerMgr, orgRegSvc)
		slog.Info("admin API enabled")
	}

	// ── HTTP Server ───────────────────────────────────────────────────────────
	apiHandler := api.NewHandler(proxyKeyRepo, proxySecretStore)
	claudeHandler := api.NewClaudeSessionHandler(sessionMgr, claudeSessionRepo)
	srv := server.New(&cfg.Server, proxyHandler, logWriter, resolver, apiHandler, claudeHandler,
		cfg.Secrets.AdminToken, adminHandler)

	// Suppress unused variable warning for metadataKeyRepo — it is available
	// for handlers added in future phases.
	_ = metadataKeyRepo

	return &Server{
		inner:         srv,
		logWriter:     logWriter,
		pricing:       pricingSvc,
		workers:       workerMgr,
		managedPoller: managedPoller,
	}, nil
}
