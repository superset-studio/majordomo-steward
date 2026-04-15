package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/migrate"
	"github.com/superset-studio/majordomo-steward/internal/secrets"
	"github.com/superset-studio/majordomo-steward/internal/steward"
	"github.com/superset-studio/majordomo-steward/internal/storage"
)

const stewardTokenPrefix = "mdm_st_"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "migrate":
			runMigrate()
			return
		case "register":
			runRegister(os.Args[2:])
			return
		}
	}
	runServe()
}

// meResponse mirrors the MeResponse type from the butler ingest package.
type meResponse struct {
	StewardID string `json:"steward_id"`
	OrgID     string `json:"org_id"`
	OrgName   string `json:"org_name"`
}

func runRegister(args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	tokenFlag := fs.String("token", "", "Steward token (mdm_st_...)")
	butlerURLFlag := fs.String("butler-url", "", "Butler base URL (e.g. https://butler.example.com)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if *tokenFlag == "" || *butlerURLFlag == "" {
		fmt.Fprintln(os.Stderr, "usage: majordomo-steward register --token <mdm_st_...> --butler-url <url>")
		os.Exit(1)
	}

	if !strings.HasPrefix(*tokenFlag, stewardTokenPrefix) || len(*tokenFlag) <= len(stewardTokenPrefix) {
		fmt.Fprintf(os.Stderr, "invalid token format: must start with %q\n", stewardTokenPrefix)
		os.Exit(1)
	}

	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if cfg.Secrets.EncryptionKey == "" {
		fmt.Fprintln(os.Stderr, "ENCRYPTION_KEY must be set to register an org")
		os.Exit(1)
	}

	secretStore, err := secrets.NewAESStore(cfg.Secrets.EncryptionKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise secret store: %v\n", err)
		os.Exit(1)
	}

	// Connect to DB and run migrations.
	db, err := sql.Open("postgres", cfg.Storage.Postgres.DSN())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}

	if err := migrate.Run(db, "./migrations"); err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}

	// Call butler's /me endpoint to get org info.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	meURL := strings.TrimRight(*butlerURLFlag, "/") + "/api/v1/steward/me"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, meURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+*tokenFlag)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to call butler /me: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "butler /me returned %d — check token and butler URL\n", resp.StatusCode)
		os.Exit(1)
	}

	var me meResponse
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		fmt.Fprintf(os.Stderr, "failed to decode butler /me response: %v\n", err)
		os.Exit(1)
	}

	// Encrypt the token before storing.
	encrypted, err := secretStore.Encrypt(*tokenFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to encrypt token: %v\n", err)
		os.Exit(1)
	}

	// Store the registered org using a thin postgres storage instance.
	store, err := storage.NewPostgresStorage(context.Background(), cfg.Storage.Postgres.DSN(), cfg.Storage.Postgres.MaxConns, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open storage: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Parse org_id from the /me response.
	orgID, err := uuid.Parse(me.OrgID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "butler returned invalid org_id %q: %v\n", me.OrgID, err)
		os.Exit(1)
	}

	if err := store.RegisterOrg(context.Background(), orgID, me.OrgName, *butlerURLFlag, encrypted); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register org: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Registered org %q (org: %s). Restart steward to activate.\n", me.OrgName, me.OrgID)
}

func runMigrate() {
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", cfg.Storage.Postgres.DSN())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}

	if err := migrate.Run(db, "./migrations"); err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("migrations applied")
}

func runServe() {
	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.Logging.Level),
	})))

	ctx := context.Background()

	srv, err := steward.Build(ctx, cfg)
	if err != nil {
		slog.Error("failed to build steward", "error", err)
		os.Exit(1)
	}

	errChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		slog.Error("server error", "error", err)
		os.Exit(1)
	case sig := <-sigChan:
		slog.Info("received signal, shutting down", "signal", sig)
	}

	if err := srv.ShutdownWithTimeout(30 * time.Second); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("steward stopped")
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
