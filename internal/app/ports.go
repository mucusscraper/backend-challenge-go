// Package app contains the use cases of the service (open wallet, submit a
// wager transaction, resume pending transactions, queries, reconciliation).
//
// HTTP handlers and the SQS consumer call the very same use cases, so both
// entry points share validation, idempotency and financial guarantees.
//
// The package depends on the domain and on the ports declared in this file;
// the PostgreSQL adapter implements them. The SQL transaction boundary is
// explicit: UnitOfWork.Run opens one database transaction and every
// repository obtained from the Tx argument participates in it, so wallet,
// transaction, ledger, inbox and outbox writes commit (or roll back)
// atomically.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
)

// UnitOfWork delimits SQL transactions.
type UnitOfWork interface {
	// Run executes fn inside a READ COMMITTED transaction with a bounded
	// lock timeout. fn's error rolls the transaction back; a nil error
	// commits it. Repositories from tx must not be used after fn returns.
	Run(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Snapshot executes fn inside a REPEATABLE READ, READ ONLY transaction:
	// every query sees the same consistent snapshot (used by reconciliation).
	Snapshot(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Tx gives access to the repositories bound to one SQL transaction.
type Tx interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// WalletRepository persists the Wallet aggregate.
type WalletRepository interface {
	// Insert stores a new wallet; ErrWalletAlreadyExists when (player,
	// currency) already has a wallet.
	Insert(ctx context.Context, w *domain.Wallet) error
	// Get loads a wallet without locking; ErrWalletNotFound if absent.
	Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// GetForUpdate loads and row-locks a wallet (SELECT ... FOR UPDATE).
	// This is the per-wallet coordination point: every writer of a wallet
	// goes through it, and wallets never share a lock.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// TryGetForUpdate is GetForUpdate with SKIP LOCKED: it returns
	// (nil, nil) when another transaction holds the lock.
	TryGetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// UpdateBalance writes balance/version/updatedAt with a compare-and-set
	// on expectedVersion; ErrConcurrentUpdate when no row matched.
	UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error
}

// TransactionRepository persists wager transactions.
type TransactionRepository interface {
	// Insert stores a transaction. Unique-constraint races are reported as
	// ErrRetryableConflict.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	// Update persists a transition, guarded by the previous status.
	Update(ctx context.Context, t *domain.WagerTransaction, expectedStatus domain.Status) error
	// Get loads a transaction; ErrNotFound if absent.
	Get(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	// FindByExternalID resolves (providerId, externalTransactionId); returns
	// (nil, nil) when absent.
	FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	// FindByIdempotency returns the transactions of the provider matching
	// either the idempotency key or the external id (0, 1 or 2 rows).
	FindByIdempotency(ctx context.Context, providerID, key, externalID string) ([]*domain.WagerTransaction, error)
	// HasSuccessfulReversal reports whether a PROCESSED REFUND or ROLLBACK
	// already targets the transaction.
	HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// ListDue returns open transactions whose next attempt is due.
	ListDue(ctx context.Context, now time.Time, limit int) ([]DueTransaction, error)
}

// DueTransaction identifies a transaction the pending worker must resume.
type DueTransaction struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

// LedgerRepository appends and reads ledger entries.
type LedgerRepository interface {
	Insert(ctx context.Context, e domain.LedgerEntry) error
	// List returns up to limit entries with seq > afterSeq, ordered by seq.
	List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	// Totals sums credits and debits (minor units) and counts entries.
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

// LedgerRow is a ledger entry with its pagination sequence.
type LedgerRow struct {
	Seq   int64
	Entry domain.LedgerEntry
}

// LedgerTotals aggregates a wallet's ledger.
type LedgerTotals struct {
	CreditsMinor int64
	DebitsMinor  int64
	Count        int64
}

// OutboxRepository stores events to be published after commit.
type OutboxRepository interface {
	Append(ctx context.Context, events ...domain.Event) error
}

// InboxRepository records completed handling of inbound messages.
type InboxRepository interface {
	// Get returns the record or (nil, nil) when absent.
	Get(ctx context.Context, consumer, messageID string) (*InboxRecord, error)
	// Insert stores the record; a duplicate is ErrRetryableConflict.
	Insert(ctx context.Context, r InboxRecord) error
}

// InboxRecord is the durable trace of a handled message.
type InboxRecord struct {
	Consumer      string
	MessageID     string
	PayloadHash   string
	TransactionID uuid.UUID
	Outcome       string
	ReceivedAt    time.Time
	CompletedAt   time.Time
}

// OutboxMessage is a claimed outbox event ready to be published. Payload is
// the immutable envelope snapshot stored at commit time.
type OutboxMessage struct {
	ID          uuid.UUID
	AggregateID string
	EventType   string
	Payload     string
	Attempts    int
	OccurredAt  time.Time
}

// OutboxStore is used by the outbox relay, outside business transactions.
type OutboxStore interface {
	// Claim leases up to limit due events to owner.
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxMessage, error)
	// MarkPublished confirms a publication.
	MarkPublished(ctx context.Context, id uuid.UUID) error
	// MarkFailed releases the lease and schedules the next attempt.
	MarkFailed(ctx context.Context, id uuid.UUID, owner, lastError string, next time.Time) error
	// Lag returns the age of the oldest unpublished event.
	Lag(ctx context.Context) (time.Duration, error)
}

// EventPublisher delivers an outbox event to the message broker.
type EventPublisher interface {
	Publish(ctx context.Context, m OutboxMessage) error
}

// Clock returns the current time (injectable for tests).
type Clock func() time.Time

// SystemClock is the production clock (UTC).
func SystemClock() time.Time { return time.Now().UTC() }

// NewUUIDv7 generates time-ordered identifiers, which keeps B-tree inserts
// local and makes ids roughly sortable by creation.
func NewUUIDv7() uuid.UUID { return uuid.Must(uuid.NewV7()) }
