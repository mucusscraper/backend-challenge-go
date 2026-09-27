package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Event types published through the transactional outbox.
const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// Aggregate types used in the envelope/outbox.
const (
	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// eventSchemaVersion is the version of every payload schema defined here.
// It is set by the constructors, never by callers.
const eventSchemaVersion = 1

// EventMeta carries the identifiers every event needs. The EventID must be
// generated once and stored in the outbox, so republishing keeps it.
type EventMeta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

// Event is an immutable integration event. It can only be built by the
// typed constructors below, which set the type and version.
type Event struct {
	id            uuid.UUID
	eventType     string
	version       int
	aggregateType string
	aggregateID   string
	correlationID string
	causationID   string
	occurredAt    time.Time
	data          any
}

// ID returns the stable event id.
func (e Event) ID() uuid.UUID { return e.id }

// Type returns the event type.
func (e Event) Type() string { return e.eventType }

// Version returns the payload schema version.
func (e Event) Version() int { return e.version }

// AggregateType returns the aggregate type.
func (e Event) AggregateType() string { return e.aggregateType }

// AggregateID returns the aggregate id.
func (e Event) AggregateID() string { return e.aggregateID }

// CorrelationID returns the correlation id.
func (e Event) CorrelationID() string { return e.correlationID }

// OccurredAt returns the event instant (UTC).
func (e Event) OccurredAt() time.Time { return e.occurredAt }

// Data returns the typed payload.
func (e Event) Data() any { return e.data }

// envelope is the JSON shape of an event on the wire.
type envelope struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	AggregateType string  `json:"aggregateType"`
	AggregateID   string  `json:"aggregateId"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId,omitempty"`
	OccurredAt    string  `json:"occurredAt"`
	Version       int     `json:"version"`
	Data          any     `json:"data"`
}

// MarshalJSON renders the envelope. Timestamps are UTC RFC 3339 (with
// milliseconds) and money is rendered as decimal strings.
func (e Event) MarshalJSON() ([]byte, error) {
	env := envelope{
		EventID:       e.id.String(),
		EventType:     e.eventType,
		AggregateType: e.aggregateType,
		AggregateID:   e.aggregateID,
		CorrelationID: e.correlationID,
		OccurredAt:    formatTime(e.occurredAt),
		Version:       e.version,
		Data:          e.data,
	}
	if e.causationID != "" {
		c := e.causationID
		env.CausationID = &c
	}
	return json.Marshal(env)
}

// formatTime renders an instant as UTC RFC 3339 with millisecond precision.
func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

func optionalTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := formatTime(t)
	return &s
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func optionalUUID(id uuid.UUID) *string {
	if id == uuid.Nil {
		return nil
	}
	s := id.String()
	return &s
}

func newEvent(meta EventMeta, eventType, aggType, aggID string, data any) (Event, error) {
	if meta.EventID == uuid.Nil {
		return Event{}, invalidArg("event id is required")
	}
	if meta.OccurredAt.IsZero() {
		return Event{}, invalidArg("event occurredAt is required")
	}
	if meta.CorrelationID == "" {
		return Event{}, invalidArg("event correlationId is required")
	}
	return Event{
		id:            meta.EventID,
		eventType:     eventType,
		version:       eventSchemaVersion,
		aggregateType: aggType,
		aggregateID:   aggID,
		correlationID: meta.CorrelationID,
		causationID:   meta.CausationID,
		occurredAt:    meta.OccurredAt.UTC(),
		data:          data,
	}, nil
}

// TransactionEventData is the common payload describing a transaction.
// External metadata is omitted for internal (OPENING) transactions.
type TransactionEventData struct {
	TransactionID                  string    `json:"transactionId"`
	Origin                         Origin    `json:"origin"`
	Kind                           Kind      `json:"kind"`
	Status                         Status    `json:"status"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	Money                          money.DTO `json:"money"`
	ProviderID                     *string   `json:"providerId,omitempty"`
	ExternalTransactionID          *string   `json:"externalTransactionId,omitempty"`
	RoundID                        *string   `json:"roundId,omitempty"`
	GameID                         *string   `json:"gameId,omitempty"`
	ReferenceExternalTransactionID *string   `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *string   `json:"referenceTransactionId,omitempty"`
}

func transactionData(t *WagerTransaction) TransactionEventData {
	return TransactionEventData{
		TransactionID:                  t.id.String(),
		Origin:                         t.origin,
		Kind:                           t.kind,
		Status:                         t.status,
		WalletID:                       t.walletID.String(),
		PlayerID:                       t.playerID.String(),
		Money:                          t.money.ToDTO(),
		ProviderID:                     optionalString(t.providerID),
		ExternalTransactionID:          optionalString(t.externalTransactionID),
		RoundID:                        optionalString(t.roundID),
		GameID:                         optionalString(t.gameID),
		ReferenceExternalTransactionID: optionalString(t.referenceExternalID),
		ReferenceTransactionID:         optionalUUID(t.referenceTransactionID),
	}
}

// WagerTransactionProcessedData is the payload of WagerTransactionProcessed.
type WagerTransactionProcessedData struct {
	TransactionEventData
	BalanceAfter money.DTO `json:"balanceAfter"`
	ProcessedAt  string    `json:"processedAt"`
}

// NewWagerTransactionProcessed builds the event for a PROCESSED transaction
// (including LOSS and the internal OPENING).
func NewWagerTransactionProcessed(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.status != StatusProcessed || !t.hasResultBalance {
		return Event{}, invalidArg("transaction must be PROCESSED")
	}
	data := WagerTransactionProcessedData{
		TransactionEventData: transactionData(t),
		BalanceAfter:         t.resultBalance.ToDTO(),
		ProcessedAt:          formatTime(t.processedAt),
	}
	return newEvent(meta, EventWagerTransactionProcessed, AggregateWagerTransaction, t.id.String(), data)
}

// WagerTransactionRejectedData is the payload of WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionEventData
	FailureCode FailureCode `json:"failureCode"`
	RejectedAt  string      `json:"rejectedAt"`
}

// NewWagerTransactionRejected builds the event for a REJECTED transaction.
func NewWagerTransactionRejected(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.status != StatusRejected {
		return Event{}, invalidArg("transaction must be REJECTED")
	}
	data := WagerTransactionRejectedData{
		TransactionEventData: transactionData(t),
		FailureCode:          t.failureCode,
		RejectedAt:           formatTime(t.processedAt),
	}
	return newEvent(meta, EventWagerTransactionRejected, AggregateWagerTransaction, t.id.String(), data)
}

// WagerTransactionPendingReferenceData is the payload of
// WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionEventData
	NextAttemptAt      *string `json:"nextAttemptAt,omitempty"`
	ReferenceExpiresAt *string `json:"referenceExpiresAt,omitempty"`
}

// NewWagerTransactionPendingReference builds the event emitted when a
// transaction starts waiting for its reference.
func NewWagerTransactionPendingReference(meta EventMeta, t *WagerTransaction) (Event, error) {
	if t.status != StatusPendingReference {
		return Event{}, invalidArg("transaction must be PENDING_REFERENCE")
	}
	data := WagerTransactionPendingReferenceData{
		TransactionEventData: transactionData(t),
		NextAttemptAt:        optionalTime(t.nextAttemptAt),
		ReferenceExpiresAt:   optionalTime(t.referenceExpires),
	}
	return newEvent(meta, EventWagerTransactionPendingReference, AggregateWagerTransaction, t.id.String(), data)
}

// WalletBalanceChangedData is the payload of WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         money.DTO `json:"money"`
	BalanceBefore money.DTO `json:"balanceBefore"`
	BalanceAfter  money.DTO `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

// NewWalletBalanceChanged builds the event from the ledger entry that
// changed the balance. The aggregate is the wallet.
func NewWalletBalanceChanged(meta EventMeta, e LedgerEntry) (Event, error) {
	if e.id == uuid.Nil {
		return Event{}, invalidArg("ledger entry is required")
	}
	data := WalletBalanceChangedData{
		WalletID:      e.walletID.String(),
		TransactionID: e.transactionID.String(),
		Direction:     e.direction,
		Money:         e.amount.ToDTO(),
		BalanceBefore: e.balanceBefore.ToDTO(),
		BalanceAfter:  e.balanceAfter.ToDTO(),
		WalletVersion: e.walletVersion,
	}
	return newEvent(meta, EventWalletBalanceChanged, AggregateWallet, e.walletID.String(), data)
}
