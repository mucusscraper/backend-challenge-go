package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// LedgerEntry is one immutable line of a wallet's append-only ledger. Every
// balance change has exactly one entry, persisted in the same SQL transaction
// as the new balance. Corrections are made with new entries (e.g. a ROLLBACK),
// never by editing existing ones; the database enforces this with triggers.
//
// All fields are unexported and there are no setters: once built an entry
// cannot change.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// LedgerEntryParams groups the attributes of a ledger entry.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	// WalletVersion is the wallet version produced by this entry. It lets
	// the database detect forks/lost updates via UNIQUE(wallet_id, version).
	WalletVersion int64
	CreatedAt     time.Time
}

// NewLedgerEntry validates and builds an entry. It enforces:
//   - non-nil identifiers, a known direction and a positive amount;
//   - one currency for amount, balanceBefore and balanceAfter;
//   - non-negative balances;
//   - balanceAfter = balanceBefore + amount (CREDIT) or - amount (DEBIT).
//
// The same function is used for rehydration, since an entry has no
// behaviour to replay: validation is all there is.
func NewLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return LedgerEntry{}, invalidArg("ledger entry identifiers are required")
	}
	if p.Direction != DirectionDebit && p.Direction != DirectionCredit {
		return LedgerEntry{}, invalidArg("ledger entry direction %q", p.Direction)
	}
	if !p.Amount.IsValid() || !p.BalanceBefore.IsValid() || !p.BalanceAfter.IsValid() {
		return LedgerEntry{}, invalidArg("ledger entry money must be initialised")
	}
	if !p.Amount.IsPositive() {
		return LedgerEntry{}, invalidArg("ledger entry amount must be positive")
	}
	if p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return LedgerEntry{}, invalidArg("ledger balances cannot be negative")
	}
	if p.WalletVersion < 1 {
		return LedgerEntry{}, invalidArg("ledger wallet version must be >= 1")
	}
	if p.CreatedAt.IsZero() {
		return LedgerEntry{}, invalidArg("ledger entry createdAt is required")
	}
	var expected money.Money
	var err error
	if p.Direction == DirectionCredit {
		expected, err = p.BalanceBefore.Add(p.Amount)
	} else {
		expected, err = p.BalanceBefore.Sub(p.Amount)
	}
	if err != nil {
		return LedgerEntry{}, invalidArg("ledger arithmetic: %v", err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return LedgerEntry{}, invalidArg("ledger arithmetic mismatch: %s %s %s != %s",
			p.BalanceBefore, p.Direction, p.Amount, p.BalanceAfter)
	}
	return LedgerEntry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		walletVersion: p.WalletVersion,
		createdAt:     p.CreatedAt.UTC(),
	}, nil
}

// ID returns the entry identifier.
func (e LedgerEntry) ID() uuid.UUID { return e.id }

// WalletID returns the wallet the entry belongs to.
func (e LedgerEntry) WalletID() uuid.UUID { return e.walletID }

// TransactionID returns the transaction that produced the entry.
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }

// Direction returns DEBIT or CREDIT.
func (e LedgerEntry) Direction() Direction { return e.direction }

// Amount returns the (positive) moved amount.
func (e LedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore returns the wallet balance before the entry.
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the wallet balance after the entry.
func (e LedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// WalletVersion returns the wallet version produced by the entry.
func (e LedgerEntry) WalletVersion() int64 { return e.walletVersion }

// CreatedAt returns the entry timestamp (UTC).
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }

// SignedAmount returns +amount for credits and -amount for debits; it is
// used by reconciliation to rebuild the balance.
func (e LedgerEntry) SignedAmount() (money.Money, error) {
	if e.direction == DirectionCredit {
		return e.amount, nil
	}
	return e.amount.Neg()
}
