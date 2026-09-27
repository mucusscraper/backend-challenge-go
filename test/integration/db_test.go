//go:build integration

// Integration tests run against the real infrastructure of docker compose:
//
//	docker compose up -d postgres keycloak localstack
//	go test -race -tags integration ./test/integration/...
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/postgres"
	tu "github.com/mucusscraper/backend-challenge-go/test/testutil"
)

// TestMigrationsUpDownUp applies, fully reverts and re-applies the
// migrations on a throwaway database.
func TestMigrationsUpDownUp(t *testing.T) {
	ctx := context.Background()
	owner := tu.OwnerPool(t)
	name := "migtest_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := owner.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = owner.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	dsn := strings.Replace(tu.OwnerDSN, "/wagering?", "/"+name+"?", 1)

	m, err := postgres.NewMigrator(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
	if v, _ := m.Version(ctx); v != 2 {
		t.Fatalf("version after up = %d", v)
	}
	if err := m.DownTo(ctx, 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','outbox_events','inbox_messages')`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d tables left after down", n)
	}
	if err := m.Up(ctx); err != nil {
		t.Fatalf("re-up: %v", err)
	}
}

func expectDBError(t *testing.T, err error, fragment string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected database error containing %q, got nil", fragment)
	}
	if !strings.Contains(err.Error(), fragment) {
		t.Fatalf("expected error containing %q, got %v", fragment, err)
	}
}

// TestLedgerIsAppendOnly: UPDATE/DELETE/TRUNCATE are refused even for the
// owner role (triggers), and the runtime role lacks the privileges anyway.
func TestLedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "100.00")
	owner := tu.OwnerPool(t)

	_, err := owner.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1`, w.ID())
	expectDBError(t, err, "append-only")
	_, err = owner.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID())
	expectDBError(t, err, "append-only")
	_, err = owner.Exec(ctx, `TRUNCATE wallet_ledger_entries`)
	expectDBError(t, err, "append-only")

	_, err = s.Pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID())
	expectDBError(t, err, "permission denied")
	_, err = s.Pool.Exec(ctx, `DELETE FROM inbox_messages`)
	expectDBError(t, err, "permission denied")
}

// TestSchemaInvariants exercises the constraints directly with SQL,
// bypassing the application.
func TestSchemaInvariants(t *testing.T) {
	ctx := context.Background()
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	owner := tu.OwnerPool(t)
	w := tu.OpenWallet(t, s, "100.00")

	t.Run("negative balance", func(t *testing.T) {
		_, err := owner.Exec(ctx, `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, w.ID())
		expectDBError(t, err, "balance_minor")
	})
	t.Run("balance change without ledger entry", func(t *testing.T) {
		_, err := owner.Exec(ctx, `UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 1 WHERE id = $1`, w.ID())
		expectDBError(t, err, "no matching ledger entry")
	})
	t.Run("version must follow balance", func(t *testing.T) {
		_, err := owner.Exec(ctx, `UPDATE wallets SET version = version + 5 WHERE id = $1`, w.ID())
		expectDBError(t, err, "version")
	})
	t.Run("duplicate wallet for player and currency", func(t *testing.T) {
		_, err := owner.Exec(ctx, `INSERT INTO wallets VALUES ($1, $2, 'BRL', 0, 1, now(), now())`, uuid.New(), w.PlayerID())
		expectDBError(t, err, "wallets_player_currency_key")
	})
	t.Run("second opening", func(t *testing.T) {
		_, err := owner.Exec(ctx, `
			INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			  result_balance_minor, created_at, updated_at, processed_at)
			VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 100, 'BRL', 100, now(), now(), now())`,
			uuid.New(), w.ID(), w.PlayerID())
		expectDBError(t, err, "wager_transactions_single_opening")
	})
	t.Run("opening with external metadata", func(t *testing.T) {
		w2 := tu.OpenWallet(t, s, "0.00")
		_, err := owner.Exec(ctx, `
			INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			  provider_id, result_balance_minor, created_at, updated_at, processed_at)
			VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 100, 'BRL', 'provider-a', 100, now(), now(), now())`,
			uuid.New(), w2.ID(), w2.PlayerID())
		expectDBError(t, err, "wager_transactions_origin_shape")
	})
	t.Run("ledger arithmetic", func(t *testing.T) {
		var txID uuid.UUID
		_ = owner.QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1`, w.ID()).Scan(&txID)
		_, err := owner.Exec(ctx, `
			INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			  balance_before_minor, balance_after_minor, wallet_version, created_at)
			VALUES ($1, $2, $3, 'DEBIT', 100, 'BRL', 1000, 950, 99, now())`, uuid.New(), w.ID(), txID)
		expectDBError(t, err, "wallet_ledger_arithmetic")
	})
	t.Run("duplicate ledger entry for transaction", func(t *testing.T) {
		var txID uuid.UUID
		_ = owner.QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1`, w.ID()).Scan(&txID)
		_, err := owner.Exec(ctx, `
			INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			  balance_before_minor, balance_after_minor, wallet_version, created_at)
			VALUES ($1, $2, $3, 'CREDIT', 1, 'BRL', 0, 1, 77, now())`, uuid.New(), w.ID(), txID)
		expectDBError(t, err, "wallet_ledger_wallet_transaction_key")
	})
	t.Run("terminal transaction is immutable", func(t *testing.T) {
		_, err := owner.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE wallet_id = $1`, w.ID())
		expectDBError(t, err, "terminal")
		_, err = owner.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id = $1`, w.ID())
		expectDBError(t, err, "cannot be deleted")
	})
	t.Run("outbox snapshot is immutable", func(t *testing.T) {
		_, err := owner.Exec(ctx, `
			UPDATE outbox_events SET payload = '{}'::json
			 WHERE id = (SELECT id FROM outbox_events ORDER BY occurred_at DESC LIMIT 1)`)
		expectDBError(t, err, "immutable")
	})
	t.Run("zero policy for BET", func(t *testing.T) {
		_, err := owner.Exec(ctx, `
			INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
			  provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, created_at, updated_at)
			VALUES ($1, 'EXTERNAL', 'BET', 'PENDING', $2, $3, 0, 'BRL', 'p', 'e', 'k', 'h', 'r', 'g', now(), now())`,
			uuid.New(), w.ID(), w.PlayerID())
		expectDBError(t, err, "wager_transactions_amount_policy")
	})
}

// TestFinancialAtomicity: a failure after the balance update rolls back the
// wallet, the transaction, the ledger entry and the outbox rows together.
func TestFinancialAtomicity(t *testing.T) {
	ctx := context.Background()
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "100.00")
	owner := tu.OwnerPool(t)

	var outboxBefore int
	_ = owner.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxBefore)

	// Simulate a crash in the middle of the unit of work: everything is
	// written, then the transaction is rolled back instead of committed.
	tx, err := owner.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	txID := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		  provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		  result_balance_minor, created_at, updated_at, processed_at)
		VALUES ($1,'EXTERNAL','BET','PROCESSED',$2,$3,1000,'BRL','provider-a',$4,$4,'h','r','g',9000,now(),now(),now())`,
		txID, w.ID(), w.PlayerID(), "atomic-"+txID.String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `UPDATE wallets SET balance_minor = 9000, version = 2 WHERE id = $1`, w.ID())
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
		  balance_before_minor, balance_after_minor, wallet_version, created_at)
		VALUES ($1, $2, $3, 'DEBIT', 1000, 'BRL', 10000, 9000, 2, now())`, uuid.New(), w.ID(), txID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, payload, occurred_at)
		VALUES ($1, 'Wallet', $2, 'WalletBalanceChanged', 1, '{}', now())`, uuid.New(), w.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)

	bal, ver := tu.StoredBalance(t, s.Pool, w.ID())
	entries, _, _ := tu.LedgerStats(t, s.Pool, w.ID())
	var outboxAfter, txCount int
	_ = owner.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxAfter)
	_ = owner.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE id = $1`, txID).Scan(&txCount)
	if bal != 10000 || ver != 1 || entries != 1 || txCount != 0 || outboxAfter < outboxBefore {
		t.Fatalf("partial state survived: bal=%d ver=%d entries=%d tx=%d", bal, ver, entries, txCount)
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestOpeningPersistence verifies the OPENING shape and events in the DB.
func TestOpeningPersistence(t *testing.T) {
	ctx := context.Background()
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "1000.00")
	var kind, status, origin string
	var provider *string
	var result int64
	err := s.Pool.QueryRow(ctx, `SELECT kind, status, origin, provider_id, result_balance_minor FROM wager_transactions WHERE wallet_id = $1`, w.ID()).
		Scan(&kind, &status, &origin, &provider, &result)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "OPENING" || status != "PROCESSED" || origin != "INTERNAL" || provider != nil || result != 100000 {
		t.Fatalf("opening row: %s %s %s %v %d", kind, status, origin, provider, result)
	}
	var types []string
	rows, _ := s.Pool.Query(ctx, `
		SELECT event_type FROM outbox_events
		 WHERE aggregate_id IN ($1, (SELECT id::text FROM wager_transactions WHERE wallet_id = $2))
		 ORDER BY event_type`, w.ID().String(), w.ID())
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		types = append(types, s)
	}
	rows.Close()
	if fmt.Sprint(types) != "[WagerTransactionProcessed WalletBalanceChanged]" {
		t.Fatalf("opening events: %v", types)
	}
	entries, net, _ := tu.LedgerStats(t, s.Pool, w.ID())
	if entries != 1 || net != 100000 {
		t.Fatalf("opening ledger: %d %d", entries, net)
	}
	// Zero opening: no OPENING, no ledger, no financial events.
	z := tu.OpenWallet(t, s, "0.00")
	var n int
	_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, z.ID()).Scan(&n)
	entries, _, _ = tu.LedgerStats(t, s.Pool, z.ID())
	if n != 0 || entries != 0 {
		t.Fatalf("zero opening created rows: tx=%d ledger=%d", n, entries)
	}
	// Duplicate wallet is a conflict.
	if _, err := s.Wallets.OpenWallet(ctx, w.PlayerID(), tu.BRL(t, "1.00"), ""); err == nil {
		t.Fatal("expected conflict for second wallet")
	}
}
