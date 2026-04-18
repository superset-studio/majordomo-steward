package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/superset-studio/majordomo-steward/internal/config"
	"github.com/superset-studio/majordomo-steward/internal/migrate"
	"github.com/superset-studio/majordomo-steward/internal/steward"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		runMigrate()
		return
	}
	runServe()
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
		slog.Error("failed to load config", "error", err)
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
