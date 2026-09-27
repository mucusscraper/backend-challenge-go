// Command migrate aplica ou reverte as migrações do banco de dados.
//
// Uso:
//
//	migrate up            aplica todas as migrações pendentes
//	migrate down          reverte a migração mais recente
//	migrate down-to N     reverte até a versão N (0 = tudo)
//	migrate status        lista as migrações e seus estados
//
// O DSN vem de MIGRATION_DATABASE_URL (role owner), com fallback para
// DATABASE_URL.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mucusscraper/backend-challenge-go/internal/postgres"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: migrate up|down|down-to N|status")
	}
	dsn := os.Getenv("MIGRATION_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		return fmt.Errorf("MIGRATION_DATABASE_URL or DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	m, err := postgres.NewMigrator(dsn)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := waitDB(ctx, m); err != nil {
		return err
	}

	switch args[0] {
	case "up":
		err = m.Up(ctx)
	case "down":
		err = m.Down(ctx)
	case "down-to":
		if len(args) < 2 {
			return fmt.Errorf("down-to requires a version")
		}
		v, perr := strconv.ParseInt(args[1], 10, 64)
		if perr != nil {
			return perr
		}
		err = m.DownTo(ctx, v)
	case "status":
		lines, serr := m.Status(ctx)
		for _, l := range lines {
			fmt.Println(l)
		}
		err = serr
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		return err
	}
	v, err := m.Version(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("schema version: %d\n", v)
	return nil
}

// waitDB tenta repetidamente até o banco de dados aceitar conexões.
func waitDB(ctx context.Context, m *postgres.Migrator) error {
	for {
		err := m.Ping(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("database not reachable: %w", err)
		case <-time.After(time.Second):
		}
	}
}
