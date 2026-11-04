package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed booking/*.sql flight/*.sql
var migrations embed.FS

// Migrate applies ordered, versioned SQL once. DDL and its version commit together.
func Migrate(ctx context.Context, pool *pgxpool.Pool, service string) error {
	files, err := fs.Glob(migrations, service+"/*.sql")
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no migrations for %q", service)
	}
	sort.Strings(files)
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(736512)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	for _, file := range files {
		var applied bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", file).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		sql, readErr := migrations.ReadFile(file)
		if readErr != nil {
			return readErr
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", file, err)
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", file); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
