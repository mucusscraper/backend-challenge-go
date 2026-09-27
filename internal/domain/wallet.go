package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Wallet is the root of the financial aggregate. Its balance can only change
// through Debit and Credit, which always return the matching LedgerEntry; the
// application layer persists both in the same SQL transaction.
//
// Concurrency: the aggregate carries a version. It starts at 1 when the
// wallet is opened and is incremented exactly once per balance change. The
// repository updates the row with "WHERE version = <loaded version>" while
// holding a row lock (SELECT ... FOR UPDATE), so a concurrent writer can
// never overwrite a committed change (no lost update).
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// InitialWalletVersion is the version of a freshly opened wallet.
const InitialWalletVersion int64 = 1

// WalletSnapshot is the full persisted state of a wallet, used to rehydrate
// it from storage.
type WalletSnapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RehydrateWallet rebuilds a wallet from persisted state. It validates the
// snapshot but does not replay any movement nor emit events.
func RehydrateWallet(s WalletSnapshot) (*Wallet, error) {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, invalidArg("wallet identifiers are required")
	}
	if !s.Balance.IsValid() {
		return nil, invalidArg("wallet balance must be initialised")
	}
	if s.Balance.IsNegative() {
		return nil, invalidArg("wallet balance cannot be negative")
	}
	if s.Version < InitialWalletVersion {
		return nil, invalidArg("wallet version must be >= 1")
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, invalidArg("wallet timestamps are required")
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		currency:  s.Balance.Currency(),
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt.UTC(),
		updatedAt: s.UpdatedAt.UTC(),
	}, nil
}

// newWallet creates a brand new wallet at version 1. It is unexported
// because wallets are only created through OpenWallet, which also produces
// the OPENING transaction, ledger entry and events when needed.
func newWallet(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, invalidArg("wallet identifiers are required")
	}
	if !initial.IsValid() {
		return nil, invalidArg("initial balance must be initialised")
	}
	if initial.IsNegative() {
		return nil, invalidArg("initial balance cannot be negative")
	}
	if now.IsZero() {
		return nil, invalidArg("creation time is required")
	}
	now = now.UTC()
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  initial.Currency(),
		balance:   initial,
		version:   InitialWalletVersion,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// ID returns the wallet identifier.
func (w *Wallet) ID() uuid.UUID { return w.id }

// PlayerID returns the owner of the wallet.
func (w *Wallet) PlayerID() uuid.UUID { return w.playerID }

// Currency returns the wallet currency.
func (w *Wallet) Currency() money.Currency { return w.currency }

// Balance returns the current balance.
func (w *Wallet) Balance() money.Money { return w.balance }

// Version returns the optimistic-concurrency version.
func (w *Wallet) Version() int64 { return w.version }

// CreatedAt returns the creation instant (UTC).
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt returns the instant of the last balance change (UTC).
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Movement describes a balance change requested on the wallet.
type Movement struct {
	EntryID       uuid.UUID
	TransactionID uuid.UUID
	Amount        money.Money
	At            time.Time
}

func (w *Wallet) validateMovement(m Movement) error {
	if m.EntryID == uuid.Nil || m.TransactionID == uuid.Nil {
		return invalidArg("movement identifiers are required")
	}
	if !m.Amount.IsValid() {
		return invalidArg("movement amount must be initialised")
	}
	if m.Amount.Currency() != w.currency {
		return reject(FailureCurrencyMismatch, "wallet currency is %s, movement is %s", w.currency, m.Amount.Currency())
	}
	if !m.Amount.IsPositive() {
		return invalidArg("movement amount must be positive")
	}
	if m.At.IsZero() {
		return invalidArg("movement time is required")
	}
	return nil
}

// Debit removes amount from the balance and returns the ledger entry. It
// returns a *RejectionError with FailureInsufficientFunds when the balance
// would become negative; the wallet is left untouched on any error.
func (w *Wallet) Debit(m Movement) (LedgerEntry, error) {
	if err := w.validateMovement(m); err != nil {
		return LedgerEntry{}, err
	}
	cmp, err := w.balance.Cmp(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	if cmp < 0 {
		return LedgerEntry{}, reject(FailureInsufficientFunds, "balance %s is lower than %s", w.balance, m.Amount)
	}
	after, err := w.balance.Sub(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	return w.apply(DirectionDebit, m, after)
}

// Credit adds amount to the balance and returns the ledger entry.
func (w *Wallet) Credit(m Movement) (LedgerEntry, error) {
	if err := w.validateMovement(m); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Add(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	return w.apply(DirectionCredit, m, after)
}

// apply builds the entry first and only mutates the wallet when the entry
// is valid, so a failure never leaves a half-applied state.
func (w *Wallet) apply(dir Direction, m Movement, after money.Money) (LedgerEntry, error) {
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            m.EntryID,
		WalletID:      w.id,
		TransactionID: m.TransactionID,
		Direction:     dir,
		Amount:        m.Amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		WalletVersion: w.version + 1,
		CreatedAt:     m.At,
	})
	if err != nil {
		return LedgerEntry{}, errors.Join(ErrInvariantViolation, err)
	}
	w.balance = after
	w.version++
	w.updatedAt = m.At.UTC()
	return entry, nil
}
