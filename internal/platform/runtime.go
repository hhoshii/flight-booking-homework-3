package platform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/hhoshii/flight-booking-homework-3/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func LogSetup() { slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil))) }

func Env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func Duration(name, fallback string) time.Duration {
	value, err := time.ParseDuration(Env(name, fallback))
	if err != nil || value <= 0 {
		panic(fmt.Sprintf("%s must be a positive duration", name))
	}
	return value
}

func PositiveInt(name, fallback string) int {
	value, err := strconv.Atoi(Env(name, fallback))
	if err != nil || value <= 0 {
		panic(fmt.Sprintf("%s must be a positive integer", name))
	}
	return value
}

func OpenDB(ctx context.Context, service string) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err == nil {
		err = db.Migrate(ctx, pool, service)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	slog.Info("database migrations applied", "service", service)
	return pool, nil
}
