package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/superset-studio/majordomo-steward/internal/api"
	"github.com/superset-studio/majordomo-steward/internal/auth"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/httputil"
	"github.com/superset-studio/majordomo-steward/internal/proxy"
)

// HealthChecker can verify that a backing resource is reachable.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Server wraps the HTTP server.
type Server struct {
	httpServer    *http.Server
	config        *config.ServerConfig
	healthChecker HealthChecker
}

// New builds and returns a fully configured Server. It wires the proxy handler
// as a catch-all at "/*" and exposes health/readiness probes. Proxy-key and
// Claude-session API routes are included when the relevant handlers are non-nil.
// Admin routes are registered when both adminToken and adminHandler are provided.
func New(
	cfg *config.ServerConfig,
	proxyHandler *proxy.Handler,
	checker HealthChecker,
	resolver *auth.Resolver,
	apiHandler *api.Handler,
	claudeHandler *api.ClaudeSessionHandler,
	adminToken string,
	adminHandler *api.AdminHandler,
) *Server {
	s := &Server{
		config:        cfg,
		healthChecker: checker,
	}

	router := chi.NewRouter()
	router.Use(Recovery)
	router.Use(RequestID)
	router.Use(Logger)

	router.Get("/health", healthHandler)
	router.Get("/readyz", s.readyzHandler)

	// Proxy-key self-service API (requires a valid Majordomo API key).
	if apiHandler != nil {
		router.Route("/api/v1", func(r chi.Router) {
			r.Use(api.AuthMiddleware(resolver))
			r.Post("/proxy-keys", apiHandler.CreateProxyKey)
			r.Get("/proxy-keys", apiHandler.ListProxyKeys)
			r.Get("/proxy-keys/{id}", apiHandler.GetProxyKey)
			r.Delete("/proxy-keys/{id}", apiHandler.RevokeProxyKey)
			r.Put("/proxy-keys/{id}/providers/{provider}", apiHandler.SetProviderMapping)
			r.Delete("/proxy-keys/{id}/providers/{provider}", apiHandler.DeleteProviderMapping)
			r.Get("/proxy-keys/{id}/providers", apiHandler.ListProviderMappings)
		})
	}

	// Claude Code session tracking API.
	if claudeHandler != nil {
		router.Route("/api/v1/claude-sessions", func(r chi.Router) {
			r.Use(api.AuthMiddleware(resolver))
			r.Post("/", claudeHandler.StartSession)
			r.Get("/", claudeHandler.ListSessions)
			r.Get("/{id}", claudeHandler.GetSession)
			r.Post("/{id}/end", claudeHandler.EndSession)
			r.Get("/{id}/requests", claudeHandler.ListSessionRequests)
		})
	}

	// Admin API — local org management (register, deregister, list orgs).
	// Only mounted when a STEWARD_ADMIN_TOKEN is configured.
	if adminToken != "" && adminHandler != nil {
		router.Route("/admin", func(r chi.Router) {
			r.Use(adminAuthMiddleware(adminToken))
			r.Get("/orgs", adminHandler.HandleListOrgs)
			r.Post("/orgs/register", adminHandler.HandleRegisterOrg)
			r.Delete("/orgs/{orgID}", adminHandler.HandleDeregisterOrg)
		})
	}

	// Catch-all: forward every other request to the LLM proxy.
	router.Handle("/*", proxyHandler)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return s
}

// Start begins listening. It blocks until the server stops.
func (s *Server) Start() error {
	slog.Info("starting server", "addr", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully drains active connections within ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down server")
	return s.httpServer.Shutdown(ctx)
}

// ShutdownWithTimeout is a convenience wrapper around Shutdown.
func (s *Server) ShutdownWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Shutdown(ctx)
}

// adminAuthMiddleware rejects requests that don't carry the correct admin bearer token.
func adminAuthMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				httputil.WriteJSONError(w, http.StatusUnauthorized, "admin token required")
				return
			}
			if after, ok := strings.CutPrefix(auth, "Bearer "); !ok || strings.TrimSpace(after) != token {
				httputil.WriteJSONError(w, http.StatusForbidden, "invalid admin token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) readyzHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := s.healthChecker.Ping(ctx); err != nil {
		slog.Warn("readiness check failed", "error", err)
		httputil.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
