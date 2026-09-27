package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Mapeamento de Money: todo valor é armazenado como BIGINT de unidades menores
// mais uma coluna CHAR(3) de moeda; a conversão passa por money.FromUnits,
// portanto nenhum float é envolvido.

// --- wallets -----------------------------------------------------------------

type walletRepo struct{ q querier }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrWalletNotFound
		}
		return nil, err
	}
	cur, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	bal, err := money.FromUnits(balance, cur)
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(domain.WalletSnapshot{
		ID: id, PlayerID: playerID, Balance: bal, Version: version, CreatedAt: createdAt, UpdatedAt: updatedAt,
	})
}

func (r walletRepo) Insert(ctx context.Context, w *domain.Wallet) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallets (`+walletColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency().Code(), w.Balance().Units(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err) && constraintName(err) == "wallets_player_currency_key" {
		return app.ErrWalletAlreadyExists
	}
	return err
}

func (r walletRepo) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

// GetForUpdate adquire o lock de linha por carteira. Outras carteiras não são
// afetadas, portanto carteiras independentes progridem em paralelo (sem lock global).
func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

func (r walletRepo) TryGetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := scanWallet(r.q.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE SKIP LOCKED`, id))
	if errors.Is(err, app.ErrWalletNotFound) {
		// Bloqueada por outra instância ou ausente; o chamador tenta novamente depois.
		return nil, nil
	}
	return w, err
}

// UpdateBalance é um compare-and-set na versão. Sob o lock de linha ela
// sempre corresponde; a condição é uma segunda guarda independente contra
// lost updates (o trigger de versão + UNIQUE(wallet_id, wallet_version) no
// ledger são a terceira).
func (r walletRepo) UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx, `
		UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Units(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentUpdate
	}
	return nil
}

// --- wager transactions ------------------------------------------------------

type transactionRepo struct{ q querier }

const transactionColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	attempts, next_attempt_at, reference_expires_at, correlation_id, created_at, updated_at, processed_at`

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id, walletID, playerID                         uuid.UUID
		origin, kind, status, currency                 string
		amount                                         int64
		providerID, externalID, key, hash, round, game *string
		refExternal, failureCode, correlationID        *string
		refID                                          *uuid.UUID
		resultBalance                                  *int64
		attempts                                       int
		nextAttempt, refExpires, processedAt           *time.Time
		createdAt, updatedAt                           time.Time
	)
	err := row.Scan(&id, &origin, &kind, &status, &walletID, &playerID, &amount, &currency,
		&providerID, &externalID, &key, &hash, &round, &game,
		&refExternal, &refID, &failureCode, &resultBalance,
		&attempts, &nextAttempt, &refExpires, &correlationID, &createdAt, &updatedAt, &processedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrNotFound
		}
		return nil, err
	}
	cur, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	amt, err := money.FromUnits(amount, cur)
	if err != nil {
		return nil, err
	}
	snap := domain.TransactionSnapshot{
		ID: id, Origin: domain.Origin(origin), Kind: domain.Kind(kind), Status: domain.Status(status),
		WalletID: walletID, PlayerID: playerID, Money: amt,
		ProviderID: deref(providerID), ExternalTransactionID: deref(externalID), IdempotencyKey: deref(key),
		PayloadHash: deref(hash), RoundID: deref(round), GameID: deref(game),
		ReferenceExternalTransactionID: deref(refExternal), ReferenceTransactionID: deref(refID),
		FailureCode: domain.FailureCode(deref(failureCode)), Attempts: attempts,
		NextAttemptAt: deref(nextAttempt), ReferenceExpiresAt: deref(refExpires),
		CorrelationID: deref(correlationID), CreatedAt: createdAt, UpdatedAt: updatedAt, ProcessedAt: deref(processedAt),
	}
	if resultBalance != nil {
		rb, err := money.FromUnits(*resultBalance, cur)
		if err != nil {
			return nil, err
		}
		snap.ResultBalance = &rb
	}
	return domain.RehydrateTransaction(snap)
}

func resultBalanceUnits(t *domain.WagerTransaction) *int64 {
	if rb, ok := t.ResultBalance(); ok {
		u := rb.Units()
		return &u
	}
	return nil
}

func (r transactionRepo) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wager_transactions (`+transactionColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Money().Units(), t.Money().Currency().Code(),
		nullString(t.ProviderID()), nullString(t.ExternalTransactionID()), nullString(t.IdempotencyKey()),
		nullString(t.PayloadHash()), nullString(t.RoundID()), nullString(t.GameID()),
		nullString(t.ReferenceExternalTransactionID()), nullUUID(t.ReferenceTransactionID()),
		nullString(string(t.FailureCode())), resultBalanceUnits(t),
		t.Attempts(), nullTime(t.NextAttemptAt()), nullTime(t.ReferenceExpiresAt()),
		nullString(t.CorrelationID()), t.CreatedAt(), t.UpdatedAt(), nullTime(t.ProcessedAt()))
	return err
}

func (r transactionRepo) Update(ctx context.Context, t *domain.WagerTransaction, expected domain.Status) error {
	tag, err := r.q.Exec(ctx, `
		UPDATE wager_transactions
		   SET status = $2, reference_transaction_id = $3, failure_code = $4, result_balance_minor = $5,
		       attempts = $6, next_attempt_at = $7, reference_expires_at = $8, updated_at = $9, processed_at = $10
		 WHERE id = $1 AND status = $11`,
		t.ID(), string(t.Status()), nullUUID(t.ReferenceTransactionID()), nullString(string(t.FailureCode())),
		resultBalanceUnits(t), t.Attempts(), nullTime(t.NextAttemptAt()), nullTime(t.ReferenceExpiresAt()),
		t.UpdatedAt(), nullTime(t.ProcessedAt()), string(expected))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentUpdate
	}
	return nil
}

func (r transactionRepo) Get(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return scanTransaction(r.q.QueryRow(ctx, `SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r transactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	t, err := scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wager_transactions
		 WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
	if errors.Is(err, app.ErrNotFound) {
		return nil, nil
	}
	return t, err
}

func (r transactionRepo) FindByIdempotency(ctx context.Context, providerID, key, externalID string) ([]*domain.WagerTransaction, error) {
	rows, err := r.q.Query(ctx, `
		SELECT `+transactionColumns+` FROM wager_transactions
		 WHERE origin = 'EXTERNAL' AND provider_id = $1
		   AND (idempotency_key = $2 OR external_transaction_id = $3)`, providerID, key, externalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r transactionRepo) HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM wager_transactions
		                WHERE reference_transaction_id = $1
		                  AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`, referenceID).Scan(&exists)
	return exists, err
}

func (r transactionRepo) ListDue(ctx context.Context, now time.Time, limit int) ([]app.DueTransaction, error) {
	rows, err := r.q.Query(ctx, `
		SELECT id, wallet_id FROM wager_transactions
		 WHERE status IN ('PENDING', 'PENDING_REFERENCE')
		   AND COALESCE(next_attempt_at, updated_at) <= $1
		 ORDER BY next_attempt_at NULLS FIRST
		 LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []app.DueTransaction
	for rows.Next() {
		var d app.DueTransaction
		if err := rows.Scan(&d.ID, &d.WalletID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// --- ledger ------------------------------------------------------------------

type ledgerRepo struct{ q querier }

func (r ledgerRepo) Insert(ctx context.Context, e domain.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallet_ledger_entries
		  (id, wallet_id, transaction_id, direction, amount_minor, currency,
		   balance_before_minor, balance_after_minor, wallet_version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Units(),
		e.Amount().Currency().Code(), e.BalanceBefore().Units(), e.BalanceAfter().Units(),
		e.WalletVersion(), e.CreatedAt())
	return err
}

func (r ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	rows, err := r.q.Query(ctx, `
		SELECT seq, id, wallet_id, transaction_id, direction, amount_minor, currency,
		       balance_before_minor, balance_after_minor, wallet_version, created_at
		  FROM wallet_ledger_entries
		 WHERE wallet_id = $1 AND seq > $2
		 ORDER BY seq
		 LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []app.LedgerRow
	for rows.Next() {
		var (
			seq, amount, before, after, version int64
			id, wID, txID                       uuid.UUID
			direction, currency                 string
			createdAt                           time.Time
		)
		if err := rows.Scan(&seq, &id, &wID, &txID, &direction, &amount, &currency, &before, &after, &version, &createdAt); err != nil {
			return nil, err
		}
		cur, err := money.ParseCurrency(currency)
		if err != nil {
			return nil, err
		}
		amt, _ := money.FromUnits(amount, cur)
		bb, _ := money.FromUnits(before, cur)
		ba, _ := money.FromUnits(after, cur)
		entry, err := domain.NewLedgerEntry(domain.LedgerEntryParams{
			ID: id, WalletID: wID, TransactionID: txID, Direction: domain.Direction(direction),
			Amount: amt, BalanceBefore: bb, BalanceAfter: ba, WalletVersion: version, CreatedAt: createdAt,
		})
		if err != nil {
			return nil, fmt.Errorf("corrupt ledger entry %s: %w", id, err)
		}
		out = append(out, app.LedgerRow{Seq: seq, Entry: entry})
	}
	return out, rows.Err()
}

// Totals soma com NUMERIC para evitar overflow de BIGINT no agregado; o
// resultado é convertido de volta e verificado para caber em int64.
func (r ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.q.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::BIGINT,
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::BIGINT,
		       COUNT(*)
		  FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&t.CreditsMinor, &t.DebitsMinor, &t.Count)
	return t, err
}

// --- outbox ------------------------------------------------------------------

type outboxRepo struct{ q querier }

// Append armazena o envelope completo de cada evento como um snapshot JSON imutável.
func (r outboxRepo) Append(ctx context.Context, events ...domain.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("outbox: marshal %s: %w", e.Type(), err)
		}
		if _, err := r.q.Exec(ctx, `
			INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version,
			                           payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			e.ID(), e.AggregateType(), e.AggregateID(), e.Type(), e.Version(), string(payload), e.OccurredAt()); err != nil {
			return err
		}
	}
	return nil
}

// --- inbox -------------------------------------------------------------------

type inboxRepo struct{ q querier }

func (r inboxRepo) Get(ctx context.Context, consumer, messageID string) (*app.InboxRecord, error) {
	var rec app.InboxRecord
	var txID *uuid.UUID
	err := r.q.QueryRow(ctx, `
		SELECT consumer_name, message_id, payload_hash, transaction_id, outcome, received_at, completed_at
		  FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&rec.Consumer, &rec.MessageID, &rec.PayloadHash, &txID, &rec.Outcome, &rec.ReceivedAt, &rec.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.TransactionID = deref(txID)
	return &rec, nil
}

func (r inboxRepo) Insert(ctx context.Context, rec app.InboxRecord) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, transaction_id, outcome, received_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rec.Consumer, rec.MessageID, rec.PayloadHash, nullUUID(rec.TransactionID), rec.Outcome, rec.ReceivedAt, rec.CompletedAt)
	return err
}
