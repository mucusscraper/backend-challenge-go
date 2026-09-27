package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// WagerTransaction records one financial operation: either the internal
// OPENING credit of a wallet or an external provider operation (BET, WIN,
// LOSS, REFUND, ROLLBACK). State is encapsulated; it changes only through
// the Mark* transition methods, which validate the state machine documented
// on Status.
type WagerTransaction struct {
	id       uuid.UUID
	origin   Origin
	kind     Kind
	status   Status
	walletID uuid.UUID
	playerID uuid.UUID
	money    money.Money

	// External-only metadata (empty for OPENING).
	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           string
	roundID               string
	gameID                string
	referenceExternalID   string

	// Resolution / outcome.
	referenceTransactionID uuid.UUID   // resolved internal reference (Nil if none)
	failureCode            FailureCode // set on REJECTED / FAILED
	resultBalance          money.Money // balance returned to the provider (valid when terminal)
	hasResultBalance       bool

	// Retry bookkeeping for PENDING_REFERENCE.
	attempts         int
	nextAttemptAt    time.Time
	referenceExpires time.Time

	correlationID string
	createdAt     time.Time
	updatedAt     time.Time
	processedAt   time.Time
}

// NewExternalTransaction creates a PENDING transaction from a validated
// provider request.
func NewExternalTransaction(id uuid.UUID, req ExternalRequest, correlationID string, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, invalidArg("transaction id is required")
	}
	if req.payloadHash == "" {
		return nil, invalidArg("external request must be built with NewExternalRequest")
	}
	if now.IsZero() {
		return nil, invalidArg("creation time is required")
	}
	now = now.UTC()
	return &WagerTransaction{
		id:                    id,
		origin:                OriginExternal,
		kind:                  req.kind,
		status:                StatusPending,
		walletID:              req.walletID,
		playerID:              req.playerID,
		money:                 req.money,
		providerID:            req.providerID,
		externalTransactionID: req.externalTransactionID,
		idempotencyKey:        req.idempotencyKey,
		payloadHash:           req.payloadHash,
		roundID:               req.roundID,
		gameID:                req.gameID,
		referenceExternalID:   req.referenceExternalID,
		correlationID:         correlationID,
		createdAt:             now,
		updatedAt:             now,
	}, nil
}

// newOpeningTransaction creates the internal OPENING credit, already
// PROCESSED: it is committed atomically with the wallet itself.
func newOpeningTransaction(id uuid.UUID, w *Wallet, correlationID string, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, invalidArg("transaction id is required")
	}
	now = now.UTC()
	return &WagerTransaction{
		id:               id,
		origin:           OriginInternal,
		kind:             KindOpening,
		status:           StatusProcessed,
		walletID:         w.id,
		playerID:         w.playerID,
		money:            w.balance,
		resultBalance:    w.balance,
		hasResultBalance: true,
		correlationID:    correlationID,
		createdAt:        now,
		updatedAt:        now,
		processedAt:      now,
	}, nil
}

// TransactionSnapshot is the persisted state used by RehydrateTransaction.
type TransactionSnapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Money                          money.Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	FailureCode                    FailureCode
	ResultBalance                  *money.Money
	Attempts                       int
	NextAttemptAt                  time.Time
	ReferenceExpiresAt             time.Time
	CorrelationID                  string
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ProcessedAt                    time.Time
}

// RehydrateTransaction rebuilds a transaction from storage. It validates the
// snapshot's consistency but performs no transition and emits no events.
func RehydrateTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, invalidArg("transaction identifiers are required")
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, err
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, err
	}
	if !s.Money.IsValid() || s.Money.IsNegative() {
		return nil, invalidArg("transaction money must be a valid non-negative amount")
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.ProviderID != "" || s.ExternalTransactionID != "" {
			return nil, invalidArg("internal transactions must be OPENING without external metadata")
		}
	case OriginExternal:
		if s.Kind == KindOpening || s.ProviderID == "" || s.ExternalTransactionID == "" ||
			s.IdempotencyKey == "" || s.PayloadHash == "" {
			return nil, invalidArg("external transaction metadata is incomplete")
		}
	default:
		return nil, invalidArg("unknown origin %q", s.Origin)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, invalidArg("%s transaction requires a failure code", s.Status)
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, invalidArg("transaction timestamps are required")
	}
	t := &WagerTransaction{
		id:                     s.ID,
		origin:                 s.Origin,
		kind:                   s.Kind,
		status:                 s.Status,
		walletID:               s.WalletID,
		playerID:               s.PlayerID,
		money:                  s.Money,
		providerID:             s.ProviderID,
		externalTransactionID:  s.ExternalTransactionID,
		idempotencyKey:         s.IdempotencyKey,
		payloadHash:            s.PayloadHash,
		roundID:                s.RoundID,
		gameID:                 s.GameID,
		referenceExternalID:    s.ReferenceExternalTransactionID,
		referenceTransactionID: s.ReferenceTransactionID,
		failureCode:            s.FailureCode,
		attempts:               s.Attempts,
		nextAttemptAt:          s.NextAttemptAt.UTC(),
		referenceExpires:       s.ReferenceExpiresAt.UTC(),
		correlationID:          s.CorrelationID,
		createdAt:              s.CreatedAt.UTC(),
		updatedAt:              s.UpdatedAt.UTC(),
		processedAt:            s.ProcessedAt.UTC(),
	}
	if s.ResultBalance != nil {
		t.resultBalance = *s.ResultBalance
		t.hasResultBalance = true
	}
	return t, nil
}

// --- transitions -----------------------------------------------------------

func (t *WagerTransaction) ensureOpen(target Status) error {
	if t.status.IsTerminal() {
		return invalidTransition(t.status, target)
	}
	return nil
}

func invalidTransition(from, to Status) error {
	return &TransitionError{From: from, To: to}
}

// TransitionError describes a refused state transition. It matches
// ErrInvalidTransition through errors.Is.
type TransitionError struct {
	From, To Status
}

// Error implements error.
func (e *TransitionError) Error() string {
	return "domain: invalid state transition " + string(e.From) + " -> " + string(e.To)
}

// Unwrap lets errors.Is(err, ErrInvalidTransition) succeed.
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

// MarkPendingReference moves the transaction to PENDING_REFERENCE (or
// reschedules it if already there). attempts is incremented on every call so
// the retry budget survives restarts once persisted.
func (t *WagerTransaction) MarkPendingReference(nextAttemptAt, expiresAt, now time.Time) error {
	if err := t.ensureOpen(StatusPendingReference); err != nil {
		return err
	}
	if t.referenceExternalID == "" {
		return invalidArg("transaction has no reference to wait for")
	}
	if !nextAttemptAt.After(now) && !nextAttemptAt.Equal(now) {
		return invalidArg("next attempt must not be in the past")
	}
	t.status = StatusPendingReference
	t.attempts++
	t.nextAttemptAt = nextAttemptAt.UTC()
	if t.referenceExpires.IsZero() {
		t.referenceExpires = expiresAt.UTC()
	}
	t.updatedAt = now.UTC()
	return nil
}

// MarkProcessed concludes the transaction successfully, storing the balance
// observed right after it (what replays return to the provider).
func (t *WagerTransaction) MarkProcessed(resultBalance money.Money, now time.Time) error {
	if err := t.ensureOpen(StatusProcessed); err != nil {
		return err
	}
	if !resultBalance.IsValid() || resultBalance.IsNegative() {
		return invalidArg("result balance must be a valid non-negative amount")
	}
	t.status = StatusProcessed
	t.resultBalance = resultBalance
	t.hasResultBalance = true
	t.nextAttemptAt = time.Time{}
	t.updatedAt = now.UTC()
	t.processedAt = now.UTC()
	return nil
}

// MarkRejected concludes the transaction with a business rejection. The
// current wallet balance is stored so replays can report it.
func (t *WagerTransaction) MarkRejected(code FailureCode, observedBalance money.Money, now time.Time) error {
	if err := t.ensureOpen(StatusRejected); err != nil {
		return err
	}
	if code == "" {
		return invalidArg("failure code is required")
	}
	t.status = StatusRejected
	t.failureCode = code
	if observedBalance.IsValid() {
		t.resultBalance = observedBalance
		t.hasResultBalance = true
	}
	t.nextAttemptAt = time.Time{}
	t.updatedAt = now.UTC()
	t.processedAt = now.UTC()
	return nil
}

// MarkFailed records a permanent infrastructure failure for audit.
func (t *WagerTransaction) MarkFailed(code FailureCode, now time.Time) error {
	if err := t.ensureOpen(StatusFailed); err != nil {
		return err
	}
	if code == "" {
		return invalidArg("failure code is required")
	}
	t.status = StatusFailed
	t.failureCode = code
	t.nextAttemptAt = time.Time{}
	t.updatedAt = now.UTC()
	t.processedAt = now.UTC()
	return nil
}

// resolveReference stores the internal id of the resolved reference.
func (t *WagerTransaction) resolveReference(id uuid.UUID) {
	t.referenceTransactionID = id
}

// --- accessors -------------------------------------------------------------

// ID returns the internal identifier.
func (t *WagerTransaction) ID() uuid.UUID { return t.id }

// Origin returns INTERNAL or EXTERNAL.
func (t *WagerTransaction) Origin() Origin { return t.origin }

// Kind returns the operation kind.
func (t *WagerTransaction) Kind() Kind { return t.kind }

// Status returns the current status.
func (t *WagerTransaction) Status() Status { return t.status }

// WalletID returns the wallet identifier.
func (t *WagerTransaction) WalletID() uuid.UUID { return t.walletID }

// PlayerID returns the player identifier.
func (t *WagerTransaction) PlayerID() uuid.UUID { return t.playerID }

// Money returns the operation amount.
func (t *WagerTransaction) Money() money.Money { return t.money }

// ProviderID returns the provider ("" for OPENING).
func (t *WagerTransaction) ProviderID() string { return t.providerID }

// ExternalTransactionID returns the provider id of the operation.
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }

// IdempotencyKey returns the key used when the operation was received.
func (t *WagerTransaction) IdempotencyKey() string { return t.idempotencyKey }

// PayloadHash returns the canonical business hash.
func (t *WagerTransaction) PayloadHash() string { return t.payloadHash }

// RoundID returns the round identifier.
func (t *WagerTransaction) RoundID() string { return t.roundID }

// GameID returns the game identifier.
func (t *WagerTransaction) GameID() string { return t.gameID }

// ReferenceExternalTransactionID returns the external reference, if any.
func (t *WagerTransaction) ReferenceExternalTransactionID() string { return t.referenceExternalID }

// ReferenceTransactionID returns the resolved internal reference (Nil if none).
func (t *WagerTransaction) ReferenceTransactionID() uuid.UUID { return t.referenceTransactionID }

// FailureCode returns the rejection/failure code ("" otherwise).
func (t *WagerTransaction) FailureCode() FailureCode { return t.failureCode }

// ResultBalance returns the balance reported to the provider and whether it
// is set.
func (t *WagerTransaction) ResultBalance() (money.Money, bool) {
	return t.resultBalance, t.hasResultBalance
}

// Attempts returns how many times the reference resolution was attempted.
func (t *WagerTransaction) Attempts() int { return t.attempts }

// NextAttemptAt returns when a PENDING_REFERENCE transaction is due.
func (t *WagerTransaction) NextAttemptAt() time.Time { return t.nextAttemptAt }

// ReferenceExpiresAt returns the TTL deadline for the reference.
func (t *WagerTransaction) ReferenceExpiresAt() time.Time { return t.referenceExpires }

// CorrelationID returns the correlation id of the originating request.
func (t *WagerTransaction) CorrelationID() string { return t.correlationID }

// CreatedAt returns the creation instant.
func (t *WagerTransaction) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt returns the last change instant.
func (t *WagerTransaction) UpdatedAt() time.Time { return t.updatedAt }

// ProcessedAt returns when a terminal state was reached (zero otherwise).
func (t *WagerTransaction) ProcessedAt() time.Time { return t.processedAt }

// SameRequest reports whether req has the same business payload.
func (t *WagerTransaction) SameRequest(req ExternalRequest) bool {
	return t.payloadHash == req.payloadHash
}
