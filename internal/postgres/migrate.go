package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for goose
	"github.com/pressly/goose/v3"

	"github.com/mucusscraper/backend-challenge-go/migrations"
)

// Migrator applies and reverts the versioned migrations embedded in the
// binary (migrations/*.sql, goose format).
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

// NewMigrator opens a dedicated connection with the migration DSN (owner
// role, not the restricted runtime role).
func NewMigrator(dsn string) (*Migrator, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}
	return &Migrator{db: db, provider: p}, nil
}

// Up applies every pending migration.
func (m *Migrator) Up(ctx context.Context) error {
	_, err := m.provider.Up(ctx)
	return err
}

// Down reverts the most recent migration.
func (m *Migrator) Down(ctx context.Context) error {
	_, err := m.provider.Down(ctx)
	return err
}

// DownTo reverts migrations down to (and excluding) version; 0 reverts all.
func (m *Migrator) DownTo(ctx context.Context, version int64) error {
	_, err := m.provider.DownTo(ctx, version)
	return err
}

// Status describes each migration and whether it is applied.
func (m *Migrator) Status(ctx context.Context) ([]string, error) {
	st, err := m.provider.Status(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(st))
	for _, s := range st {
		out = append(out, fmt.Sprintf("%05d %-40s %s", s.Source.Version, s.Source.Path, s.State))
	}
	return out, nil
}

// Version returns the current schema version.
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	return m.provider.GetDBVersion(ctx)
}

// Ping checks connectivity.
func (m *Migrator) Ping(ctx context.Context) error { return m.db.PingContext(ctx) }

// Close releases the connection.
func (m *Migrator) Close() error { return m.db.Close() }
