package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Tipos de evento publicados pelo outbox transacional.
const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// Tipos de agregado usados no envelope/outbox.
const (
	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// eventSchemaVersion é a versão de todos os schemas de payload definidos aqui.
// É definido pelos construtores, nunca pelos chamadores.
const eventSchemaVersion = 1

// EventMeta carrega os identificadores que todo evento precisa. O EventID deve
// ser gerado uma vez e armazenado no outbox, para que a republicação o mantenha.
type EventMeta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

// Event é um evento de integração imutável. Só pode ser construído pelos
// construtores tipados abaixo, que definem o tipo e a versão.
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

// ID retorna o id estável do evento.
func (e Event) ID() uuid.UUID { return e.id }

// Type retorna o tipo do evento.
func (e Event) Type() string { return e.eventType }

// Version retorna a versão do schema do payload.
func (e Event) Version() int { return e.version }

// AggregateType retorna o tipo do agregado.
func (e Event) AggregateType() string { return e.aggregateType }

// AggregateID retorna o id do agregado.
func (e Event) AggregateID() string { return e.aggregateID }

// CorrelationID retorna o correlation id.
func (e Event) CorrelationID() string { return e.correlationID }

// OccurredAt retorna o instante do evento (UTC).
func (e Event) OccurredAt() time.Time { return e.occurredAt }

// Data retorna o payload tipado.
func (e Event) Data() any { return e.data }

// envelope é o formato JSON de um evento em trânsito.
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

// MarshalJSON renderiza o envelope. Timestamps são UTC RFC 3339 (com
// milissegundos) e money é renderizado como strings decimais.
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

// formatTime renderiza um instante como UTC RFC 3339 com precisão de milissegundos.
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

// TransactionEventData é o payload comum que descreve uma transação.
// Metadados externos são omitidos para transações internas (OPENING).
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

// WagerTransactionProcessedData é o payload de WagerTransactionProcessed.
type WagerTransactionProcessedData struct {
	TransactionEventData
	BalanceAfter money.DTO `json:"balanceAfter"`
	ProcessedAt  string    `json:"processedAt"`
}

// NewWagerTransactionProcessed constrói o evento para uma transação PROCESSED
// (incluindo LOSS e o OPENING interno).
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

// WagerTransactionRejectedData é o payload de WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionEventData
	FailureCode FailureCode `json:"failureCode"`
	RejectedAt  string      `json:"rejectedAt"`
}

// NewWagerTransactionRejected constrói o evento para uma transação REJECTED.
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

// WagerTransactionPendingReferenceData é o payload de
// WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionEventData
	NextAttemptAt      *string `json:"nextAttemptAt,omitempty"`
	ReferenceExpiresAt *string `json:"referenceExpiresAt,omitempty"`
}

// NewWagerTransactionPendingReference constrói o evento emitido quando uma
// transação começa a aguardar sua referência.
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

// WalletBalanceChangedData é o payload de WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         money.DTO `json:"money"`
	BalanceBefore money.DTO `json:"balanceBefore"`
	BalanceAfter  money.DTO `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

// NewWalletBalanceChanged constrói o evento a partir da entrada do ledger que
// mudou o saldo. O agregado é a carteira.
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
