package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// IDGenerator produces new unique identifiers (UUIDv7 in production).
type IDGenerator func() uuid.UUID

// ReferencePolicy bounds how long a transaction may wait for a reference
// that has not arrived yet (or is itself still pending).
type ReferencePolicy struct {
	// MaxAttempts is the maximum number of resolution attempts, counting the
	// first synchronous one.
	MaxAttempts int
	// TTL is the maximum time since the transaction was created.
	TTL time.Duration
	// BaseBackoff is the delay after the first attempt; it doubles on every
	// attempt (exponential backoff) up to MaxBackoff.
	BaseBackoff time.Duration
	// MaxBackoff caps the delay between attempts.
	MaxBackoff time.Duration
}

// DefaultReferencePolicy is used when no policy is configured.
var DefaultReferencePolicy = ReferencePolicy{
	MaxAttempts: 10,
	TTL:         10 * time.Minute,
	BaseBackoff: 500 * time.Millisecond,
	MaxBackoff:  60 * time.Second,
}

// Delay returns the backoff before attempt number attempt+1 (attempt >= 1).
func (p ReferencePolicy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	if d > p.MaxBackoff {
		return p.MaxBackoff
	}
	return d
}

// ReferenceState is what the application layer found when resolving
// (providerId, referenceExternalTransactionId).
type ReferenceState struct {
	// Transaction is the referenced transaction, nil when not found.
	Transaction *WagerTransaction
	// AlreadyReversed is true when a PROCESSED REFUND or ROLLBACK already
	// targets the reference. The database enforces the same rule with a
	// partial unique index, so a race cannot produce two reversals.
	AlreadyReversed bool
}

// SettleContext carries the non-deterministic inputs of Settle.
type SettleContext struct {
	Now         time.Time
	NewID       IDGenerator
	CausationID string
	Policy      ReferencePolicy
}

// Settlement is the outcome of Settle: the ledger entry to append (if the
// balance changed) and the events to store in the outbox. The transaction
// and wallet passed to Settle are mutated in place.
type Settlement struct {
	Entry  *LedgerEntry
	Events []Event
}

// Settle applies an open (PENDING or PENDING_REFERENCE) transaction to its
// wallet. It encodes every business rule of the five external kinds:
//
//   - BET: debit, positive amount, sufficient balance (else INSUFFICIENT_FUNDS);
//   - WIN: credit; an optional reference must be a BET of the same round;
//   - LOSS: no movement, no ledger entry, no version change;
//   - REFUND: credit of the full amount of a PROCESSED BET;
//   - ROLLBACK: opposite movement of a PROCESSED BET, WIN or REFUND, full
//     amount; a debit that would make the balance negative is rejected with
//     REVERSAL_INSUFFICIENT_FUNDS.
//
// A reference that is missing (or still pending) moves the transaction to
// PENDING_REFERENCE with exponential backoff until the policy expires, then
// the transaction is REJECTED.
//
// Business refusals are not returned as errors: they produce a REJECTED
// transaction plus a WagerTransactionRejected event. An error is returned
// only for invalid calls or broken invariants.
func Settle(t *WagerTransaction, w *Wallet, ref ReferenceState, c SettleContext) (Settlement, error) {
	if t == nil || w == nil || c.NewID == nil || c.Now.IsZero() {
		return Settlement{}, invalidArg("settle requires transaction, wallet, id generator and time")
	}
	if t.status.IsTerminal() {
		return Settlement{}, invalidTransition(t.status, StatusProcessed)
	}
	if t.origin != OriginExternal {
		return Settlement{}, invalidArg("only external transactions are settled")
	}
	if t.walletID != w.id {
		return Settlement{}, errors.Join(ErrInvariantViolation, invalidArg("wallet does not match transaction"))
	}
	s := settler{t: t, w: w, ref: ref, c: c}
	return s.run()
}

type settler struct {
	t   *WagerTransaction
	w   *Wallet
	ref ReferenceState
	c   SettleContext
	out Settlement
}

func (s *settler) run() (Settlement, error) {
	t, w := s.t, s.w
	if t.playerID != w.playerID {
		return s.rejectWith(FailurePlayerWalletMismatch)
	}
	if t.money.Currency() != w.currency {
		return s.rejectWith(FailureCurrencyMismatch)
	}

	var refTx *WagerTransaction
	if t.referenceExternalID != "" {
		ready, code, err := s.checkReference()
		if err != nil || code != "" || !ready {
			if err != nil {
				return Settlement{}, err
			}
			if code != "" {
				return s.rejectWith(code)
			}
			return s.out, nil // pending
		}
		refTx = s.ref.Transaction
		t.resolveReference(refTx.id)
	}

	direction, moves := movementFor(t.kind, refTx)
	if !moves {
		return s.processed(nil)
	}
	mv := Movement{EntryID: s.c.NewID(), TransactionID: t.id, Amount: t.money, At: s.c.Now}
	var entry LedgerEntry
	var err error
	if direction == DirectionDebit {
		entry, err = w.Debit(mv)
	} else {
		entry, err = w.Credit(mv)
	}
	if err != nil {
		var rej *RejectionError
		if errors.As(err, &rej) {
			code := rej.Code
			if code == FailureInsufficientFunds && t.kind == KindRollback {
				code = FailureReversalInsufficientFunds
			}
			return s.rejectWith(code)
		}
		return Settlement{}, err
	}
	return s.processed(&entry)
}

// movementFor returns the direction of the wallet movement for a kind, and
// false when the kind does not move money (LOSS).
func movementFor(kind Kind, ref *WagerTransaction) (Direction, bool) {
	switch kind {
	case KindBet:
		return DirectionDebit, true
	case KindWin, KindRefund:
		return DirectionCredit, true
	case KindRollback:
		d, _ := movementFor(ref.kind, nil)
		return d.Opposite(), true
	default: // LOSS
		return "", false
	}
}

// allowedReferenceKinds lists what each kind may reference.
var allowedReferenceKinds = map[Kind][]Kind{
	KindWin:      {KindBet},
	KindRefund:   {KindBet},
	KindRollback: {KindBet, KindWin, KindRefund},
}

// checkReference validates the reference. It returns ready=true when the
// operation can proceed, a failure code for a definitive rejection, or
// neither when the transaction was moved to PENDING_REFERENCE.
func (s *settler) checkReference() (ready bool, code FailureCode, err error) {
	t, ref := s.t, s.ref.Transaction
	if ref == nil {
		return false, "", s.waitForReference(FailureReferenceNotFound)
	}
	if !ref.status.IsTerminal() {
		return false, "", s.waitForReference(FailureReferenceNotProcessed)
	}
	if ref.status != StatusProcessed {
		return false, FailureReferenceNotProcessed, nil
	}
	if !kindAllowed(t.kind, ref.kind) {
		return false, FailureReferenceKindNotAllowed, nil
	}
	if ref.providerID != t.providerID || ref.playerID != t.playerID || ref.walletID != t.walletID ||
		ref.money.Currency() != t.money.Currency() || ref.roundID != t.roundID {
		return false, FailureReferenceMismatch, nil
	}
	if t.kind.IsReversal() {
		if !ref.money.Equal(t.money) {
			return false, FailureReversalAmountMismatch, nil
		}
		if s.ref.AlreadyReversed {
			return false, FailureReferenceAlreadyReversed, nil
		}
	}
	return true, "", nil
}

func kindAllowed(kind, refKind Kind) bool {
	for _, k := range allowedReferenceKinds[kind] {
		if k == refKind {
			return true
		}
	}
	return false
}

// waitForReference reschedules the transaction or, when the retry budget or
// TTL is exhausted, rejects it with expiredCode.
func (s *settler) waitForReference(expiredCode FailureCode) error {
	t, p, now := s.t, s.c.Policy, s.c.Now
	if p.MaxAttempts <= 0 {
		p = DefaultReferencePolicy
	}
	expiresAt := t.referenceExpires
	if expiresAt.IsZero() {
		expiresAt = t.createdAt.Add(p.TTL)
	}
	if t.attempts+1 >= p.MaxAttempts || !now.Before(expiresAt) {
		_, err := s.rejectWith(expiredCode)
		return err
	}
	wasPending := t.status == StatusPending
	if err := t.MarkPendingReference(now.Add(p.Delay(t.attempts+1)), expiresAt, now); err != nil {
		return err
	}
	if wasPending {
		ev, err := NewWagerTransactionPendingReference(s.meta(), t)
		if err != nil {
			return err
		}
		s.out.Events = append(s.out.Events, ev)
	}
	return nil
}

func (s *settler) meta() EventMeta {
	return EventMeta{
		EventID:       s.c.NewID(),
		CorrelationID: s.correlationID(),
		CausationID:   s.c.CausationID,
		OccurredAt:    s.c.Now,
	}
}

func (s *settler) correlationID() string {
	if s.t.correlationID != "" {
		return s.t.correlationID
	}
	return s.t.id.String()
}

func (s *settler) rejectWith(code FailureCode) (Settlement, error) {
	if err := s.t.MarkRejected(code, s.w.balance, s.c.Now); err != nil {
		return Settlement{}, err
	}
	ev, err := NewWagerTransactionRejected(s.meta(), s.t)
	if err != nil {
		return Settlement{}, err
	}
	s.out.Events = append(s.out.Events, ev)
	return s.out, nil
}

func (s *settler) processed(entry *LedgerEntry) (Settlement, error) {
	if err := s.t.MarkProcessed(s.w.balance, s.c.Now); err != nil {
		return Settlement{}, err
	}
	ev, err := NewWagerTransactionProcessed(s.meta(), s.t)
	if err != nil {
		return Settlement{}, err
	}
	s.out.Events = append(s.out.Events, ev)
	if entry != nil {
		meta := s.meta()
		meta.CausationID = s.t.id.String()
		bc, err := NewWalletBalanceChanged(meta, *entry)
		if err != nil {
			return Settlement{}, err
		}
		s.out.Entry = entry
		s.out.Events = append(s.out.Events, bc)
	}
	return s.out, nil
}

// FailPermanently marks an open transaction as FAILED (infrastructure
// failure recorded for audit). No event is emitted: FAILED is an internal
// audit state, not a business outcome.
func FailPermanently(t *WagerTransaction, now time.Time) error {
	return t.MarkFailed(FailureProcessingFailed, now)
}

// OpenWalletParams groups the inputs of OpenWallet.
type OpenWalletParams struct {
	WalletID       uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
	Now            time.Time
	NewID          IDGenerator
}

// Opening is the result of opening a wallet. For a positive initial balance
// it contains the internal OPENING transaction (already PROCESSED), its
// CREDIT ledger entry (version 1, 0.00 -> initial) and the
// WagerTransactionProcessed + WalletBalanceChanged events. For a zero
// initial balance only the wallet is created.
type Opening struct {
	Wallet      *Wallet
	Transaction *WagerTransaction
	Entry       *LedgerEntry
	Events      []Event
}

// OpenWallet creates a wallet (version 1) and, when the initial balance is
// positive, everything that must be committed with it.
func OpenWallet(p OpenWalletParams) (Opening, error) {
	if p.NewID == nil {
		return Opening{}, invalidArg("id generator is required")
	}
	w, err := newWallet(p.WalletID, p.PlayerID, p.InitialBalance, p.Now)
	if err != nil {
		return Opening{}, err
	}
	out := Opening{Wallet: w}
	if p.InitialBalance.IsZero() {
		return out, nil
	}
	correlationID := p.CorrelationID
	if correlationID == "" {
		correlationID = w.id.String()
	}
	tx, err := newOpeningTransaction(p.NewID(), w, correlationID, p.Now)
	if err != nil {
		return Opening{}, err
	}
	zero, err := money.Zero(w.currency)
	if err != nil {
		return Opening{}, err
	}
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            p.NewID(),
		WalletID:      w.id,
		TransactionID: tx.id,
		Direction:     DirectionCredit,
		Amount:        w.balance,
		BalanceBefore: zero,
		BalanceAfter:  w.balance,
		WalletVersion: InitialWalletVersion,
		CreatedAt:     p.Now,
	})
	if err != nil {
		return Opening{}, err
	}
	processed, err := NewWagerTransactionProcessed(EventMeta{
		EventID: p.NewID(), CorrelationID: correlationID, OccurredAt: p.Now,
	}, tx)
	if err != nil {
		return Opening{}, err
	}
	changed, err := NewWalletBalanceChanged(EventMeta{
		EventID: p.NewID(), CorrelationID: correlationID, CausationID: tx.id.String(), OccurredAt: p.Now,
	}, entry)
	if err != nil {
		return Opening{}, err
	}
	out.Transaction = tx
	out.Entry = &entry
	out.Events = []Event{processed, changed}
	return out, nil
}
