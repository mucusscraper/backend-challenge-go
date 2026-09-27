package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver for goose
	"github.com/pressly/goose/v3"

	"github.com/mucusscraper/backend-challenge-go/migrations"
)

// Migrator aplica e reverte as migrações versionadas embutidas no binário
// (migrations/*.sql, formato goose).
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

// NewMigrator abre uma conexão dedicada com o DSN de migração (role owner,
// não o role restrito de runtime).
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

// Up aplica todas as migrações pendentes.
func (m *Migrator) Up(ctx context.Context) error {
	_, err := m.provider.Up(ctx)
	return err
}

// Down reverte a migração mais recente.
func (m *Migrator) Down(ctx context.Context) error {
	_, err := m.provider.Down(ctx)
	return err
}

// DownTo reverte migrações até (exclusive) a versão; 0 reverte todas.
func (m *Migrator) DownTo(ctx context.Context, version int64) error {
	_, err := m.provider.DownTo(ctx, version)
	return err
}

// Status descreve cada migração e se ela está aplicada.
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

// Version retorna a versão atual do schema.
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	return m.provider.GetDBVersion(ctx)
}

// Ping verifica a conectividade.
func (m *Migrator) Ping(ctx context.Context) error { return m.db.PingContext(ctx) }

// Close libera a conexão.
func (m *Migrator) Close() error { return m.db.Close() }
