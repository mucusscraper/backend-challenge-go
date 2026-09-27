// Package postgres implementa as portas de aplicação com pgx e SQL explícito.
// Transações, bloqueios de linha e tratamento de restrições são todos visíveis
// neste pacote; não há ORM.
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

// NewPool constrói um pool pgx. As conexões são estabelecidas de forma lazy;
// o hook de ciclo de vida do fx (ver bootstrap) faz ping com retries na
// inicialização e fecha o pool no desligamento, após todo componente que o usa
// ter parado.
//
// lock_timeout é definido como parâmetro de sessão, para que nenhuma transação
// espere indefinidamente por um lock de carteira; um timeout aparece como app.ErrTransient.
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

// WaitReady faz ping no banco de dados até que ele responda ou o timeout expire.
// Torna a inicialização resiliente a um banco ainda inicializando.
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

// querier é satisfeito por pgx.Tx e *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// UnitOfWork implementa app.UnitOfWork em um pool pgx.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constrói a unidade de trabalho.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

var _ app.UnitOfWork = (*UnitOfWork)(nil)

// Run executa fn em uma transação READ COMMITTED. O isolamento depende de
// bloqueios explícitos de linha (SELECT ... FOR UPDATE na carteira) em vez de
// SERIALIZABLE, mantendo a contenção por carteira e evitando falhas de
// serialização globais.
func (u *UnitOfWork) Run(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// Snapshot executa fn em uma transação REPEATABLE READ, READ ONLY.
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
			// Rollback com um contexto novo: o do chamador pode estar cancelado.
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

// txRepos vincula todos os repositórios a uma transação pgx.
type txRepos struct {
	q querier
}

func (r *txRepos) Wallets() app.WalletRepository           { return walletRepo{q: r.q} }
func (r *txRepos) Transactions() app.TransactionRepository { return transactionRepo{q: r.q} }
func (r *txRepos) Ledger() app.LedgerRepository            { return ledgerRepo{q: r.q} }
func (r *txRepos) Outbox() app.OutboxRepository            { return outboxRepo{q: r.q} }
func (r *txRepos) Inbox() app.InboxRepository              { return inboxRepo{q: r.q} }

// Códigos de erro PostgreSQL usados para classificação.
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

// mapErr classifica erros de banco de dados em erros de aplicação:
//   - violação única, deadlock, falha de serialização -> ErrRetryableConflict
//   - timeout de lock, cancelamento, problemas de conexão -> ErrTransient
//   - qualquer outra coisa (violações de check, erros de trigger) -> inalterado (permanente)
//
// Erros já classificados pela aplicação são retornados sem alteração.
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
		if len(pgErr.Code) == 2+3 && pgErr.Code[:2] == "08" { // classe de exceção de conexão
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
	// Erros restantes não-PostgreSQL do driver são falhas de rede/IO
	// (reset de conexão, resultado de commit desconhecido...). Retry é
	// seguro porque toda operação é idempotente.
	if isDriverIOError(err) {
		return fmt.Errorf("%w: %v", app.ErrTransient, err)
	}
	return err
}

// isDriverIOError reconhece falhas de nível de conexão reportadas pelo pgx.
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

// constraintName retorna a restrição violada de um erro PostgreSQL.
func constraintName(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

// isUniqueViolation indica um erro 23505.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == codeUniqueViolation
}
