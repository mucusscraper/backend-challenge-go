// Package postgres implements the application ports with pgx and explicit
// SQL. Transactions, row locks and constraint handling are all visible in
// this package; there is no ORM.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
)

// NewPool builds a pgx pool. Connections are established lazily; the fx
// lifecycle hook (see bootstrap) pings with retries at startup and closes
// the pool at shutdown, after every component using it has stopped.
//
// lock_timeout is set as a session parameter, so no transaction can wait
// indefinitely for a wallet lock; a timeout surfaces as app.ErrTransient.
func NewPool(cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pc.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(cfg.LockTimeout.Milliseconds(), 10)
	pc.ConnConfig.RuntimeParams["application_name"] = "wallet-service"
	pc.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	return pgxpool.NewWithConfig(context.Background(), pc)
}

// WaitReady pings the database until it answers or timeout elapses. It
// makes startup resilient to a database that is still booting.
func WaitReady(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	delay := 200 * time.Millisecond
	for {
		err := pool.Ping(ctx)
		if err == nil {
			return nil
		}
		log.WarnContext(ctx, "database not ready yet", "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres: not ready after %s: %w", timeout, err)
		case <-time.After(delay):
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

// querier is satisfied by pgx.Tx and *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// UnitOfWork implements app.UnitOfWork on a pgx pool.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork builds the unit of work.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// Run executes fn in a READ COMMITTED transaction. Isolation relies on
// explicit row locks (SELECT ... FOR UPDATE on the wallet) rather than on
// SERIALIZABLE, which keeps contention per wallet and avoids global
// serialization failures.
func (u *UnitOfWork) Run(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// Snapshot executes fn in a REPEATABLE READ, READ ONLY transaction.
func (u *UnitOfWork) Snapshot(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context, tx app.Tx) error) (err error) {
	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return mapErr(err)
	}
	defer func() {
		if err != nil {
			// Rollback with a fresh context: the caller's may be cancelled.
			rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rbCtx)
		}
	}()
	if err = fn(ctx, &txRepos{q: tx}); err != nil {
		return mapErr(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return mapErr(err)
	}
	return nil
}

// txRepos binds every repository to one pgx transaction.
type txRepos struct {
	q querier
}

func (r *txRepos) Wallets() app.WalletRepository           { return walletRepo{q: r.q} }
func (r *txRepos) Transactions() app.TransactionRepository { return transactionRepo{q: r.q} }
func (r *txRepos) Ledger() app.LedgerRepository            { return ledgerRepo{q: r.q} }
func (r *txRepos) Outbox() app.OutboxRepository            { return outboxRepo{q: r.q} }
func (r *txRepos) Inbox() app.InboxRepository              { return inboxRepo{q: r.q} }

// PostgreSQL error codes used for classification.
const (
	codeUniqueViolation      = "23505"
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
	codeLockNotAvailable     = "55P03"
	codeQueryCanceled        = "57014"
	codeAdminShutdown        = "57P01"
	codeCrashShutdown        = "57P02"
	codeCannotConnectNow     = "57P03"
	codeTooManyConnections   = "53300"
	codeReadOnlyTransaction  = "25006"
)

// mapErr classifies database errors into application errors:
//   - unique violation, deadlock, serialization failure -> ErrRetryableConflict
//   - lock timeout, cancellation, connection problems   -> ErrTransient
//   - anything else (check violations, trigger errors)   -> unchanged (permanent)
//
// Errors already classified by the application are returned untouched.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, app.ErrRetryableConflict) || errors.Is(err, app.ErrTransient) ||
		errors.Is(err, app.ErrConcurrentUpdate) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case codeUniqueViolation, codeSerializationFailure, codeDeadlockDetected:
			return fmt.Errorf("%w: %s (%s)", app.ErrRetryableConflict, pgErr.Message, pgErr.ConstraintName)
		case codeLockNotAvailable, codeQueryCanceled, codeAdminShutdown, codeCrashShutdown,
			codeCannotConnectNow, codeTooManyConnections:
			return fmt.Errorf("%w: %s", app.ErrTransient, pgErr.Message)
		}
		if len(pgErr.Code) == 2+3 && pgErr.Code[:2] == "08" { // connection exception class
			return fmt.Errorf("%w: %s", app.ErrTransient, pgErr.Message)
		}
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %v", app.ErrTransient, err)
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) || pgconn.Timeout(err) || pgconn.SafeToRetry(err) {
		return fmt.Errorf("%w: %v", app.ErrTransient, err)
	}
	// Remaining non-PostgreSQL errors from the driver are network/IO
	// failures (connection reset, commit outcome unknown...). Retrying is
	// safe because every operation is idempotent.
	if isDriverIOError(err) {
		return fmt.Errorf("%w: %v", app.ErrTransient, err)
	}
	return err
}

// isDriverIOError recognises connection-level failures reported by pgx.
func isDriverIOError(err error) bool {
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, pgx.ErrTxClosed) || pgconn.Timeout(err) ||
		containsAny(err.Error(), "conn closed", "connection reset", "broken pipe", "unexpected EOF",
			"failed to connect", "connection refused", "closed pool")
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// constraintName returns the violated constraint of a PostgreSQL error.
func constraintName(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

// isUniqueViolation reports a 23505 error.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == codeUniqueViolation
}
