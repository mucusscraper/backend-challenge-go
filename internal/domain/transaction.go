package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// WagerTransaction registra uma operação financeira: o crédito OPENING interno
// de uma carteira ou uma operação externa de provedor (BET, WIN, LOSS, REFUND,
// ROLLBACK). O estado é encapsulado; muda apenas pelos métodos de transição
// Mark*, que validam a máquina de estados documentada em Status.
type WagerTransaction struct {
	id       uuid.UUID
	origin   Origin
	kind     Kind
	status   Status
	walletID uuid.UUID
	playerID uuid.UUID
	money    money.Money

	// Metadados exclusivos de operações externas (vazio para OPENING).
	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           string
	roundID               string
	gameID                string
	referenceExternalID   string

	// Resolução / resultado.
	referenceTransactionID uuid.UUID   // referência interna resolvida (Nil se ausente)
	failureCode            FailureCode // definido em REJECTED / FAILED
	resultBalance          money.Money // saldo retornado ao provedor (válido quando terminal)
	hasResultBalance       bool

	// Bookkeeping de retry para PENDING_REFERENCE.
	attempts         int
	nextAttemptAt    time.Time
	referenceExpires time.Time

	correlationID string
	createdAt     time.Time
	updatedAt     time.Time
	processedAt   time.Time
}

// NewExternalTransaction cria uma transação PENDING a partir de uma
// requisição de provedor validada.
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

// newOpeningTransaction cria o crédito OPENING interno, já PROCESSED:
// é commitado atomicamente com a própria carteira.
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

// TransactionSnapshot é o estado persistido usado por RehydrateTransaction.
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

// RehydrateTransaction reconstrói uma transação a partir do armazenamento.
// Valida a consistência do snapshot mas não realiza transições e não emite eventos.
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

// --- transições -----------------------------------------------------------

func (t *WagerTransaction) ensureOpen(target Status) error {
	if t.status.IsTerminal() {
		return invalidTransition(t.status, target)
	}
	return nil
}

func invalidTransition(from, to Status) error {
	return &TransitionError{From: from, To: to}
}

// TransitionError descreve uma transição de estado recusada. Corresponde a
// ErrInvalidTransition via errors.Is.
type TransitionError struct {
	From, To Status
}

// Error implementa error.
func (e *TransitionError) Error() string {
	return "domain: invalid state transition " + string(e.From) + " -> " + string(e.To)
}

// Unwrap faz errors.Is(err, ErrInvalidTransition) ter sucesso.
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

// MarkPendingReference move a transação para PENDING_REFERENCE (ou a
// reagenda se já estiver lá). attempts é incrementado a cada chamada para
// que o orçamento de retry sobreviva a reinicializações após persistido.
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

// MarkProcessed conclui a transação com sucesso, armazenando o saldo
// observado logo após ela (o que os replays retornam ao provedor).
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

// MarkRejected conclui a transação com uma rejeição de negócio. O saldo
// atual da carteira é armazenado para que os replays possam reportá-lo.
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

// MarkFailed registra uma falha permanente de infraestrutura para auditoria.
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

// resolveReference armazena o id interno da referência resolvida.
func (t *WagerTransaction) resolveReference(id uuid.UUID) {
	t.referenceTransactionID = id
}

// --- acessores -------------------------------------------------------------

// ID retorna o identificador interno.
func (t *WagerTransaction) ID() uuid.UUID { return t.id }

// Origin retorna INTERNAL ou EXTERNAL.
func (t *WagerTransaction) Origin() Origin { return t.origin }

// Kind retorna o tipo da operação.
func (t *WagerTransaction) Kind() Kind { return t.kind }

// Status retorna o status atual.
func (t *WagerTransaction) Status() Status { return t.status }

// WalletID retorna o identificador da carteira.
func (t *WagerTransaction) WalletID() uuid.UUID { return t.walletID }

// PlayerID retorna o identificador do jogador.
func (t *WagerTransaction) PlayerID() uuid.UUID { return t.playerID }

// Money retorna o valor da operação.
func (t *WagerTransaction) Money() money.Money { return t.money }

// ProviderID retorna o provedor ("" para OPENING).
func (t *WagerTransaction) ProviderID() string { return t.providerID }

// ExternalTransactionID retorna o id da operação no provedor.
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }

// IdempotencyKey retorna a chave usada quando a operação foi recebida.
func (t *WagerTransaction) IdempotencyKey() string { return t.idempotencyKey }

// PayloadHash retorna o hash canônico de negócio.
func (t *WagerTransaction) PayloadHash() string { return t.payloadHash }

// RoundID retorna o identificador da rodada.
func (t *WagerTransaction) RoundID() string { return t.roundID }

// GameID retorna o identificador do jogo.
func (t *WagerTransaction) GameID() string { return t.gameID }

// ReferenceExternalTransactionID retorna a referência externa, se houver.
func (t *WagerTransaction) ReferenceExternalTransactionID() string { return t.referenceExternalID }

// ReferenceTransactionID retorna a referência interna resolvida (Nil se ausente).
func (t *WagerTransaction) ReferenceTransactionID() uuid.UUID { return t.referenceTransactionID }

// FailureCode retorna o código de rejeição/falha ("" caso contrário).
func (t *WagerTransaction) FailureCode() FailureCode { return t.failureCode }

// ResultBalance retorna o saldo reportado ao provedor e se está definido.
func (t *WagerTransaction) ResultBalance() (money.Money, bool) {
	return t.resultBalance, t.hasResultBalance
}

// Attempts retorna quantas vezes a resolução da referência foi tentada.
func (t *WagerTransaction) Attempts() int { return t.attempts }

// NextAttemptAt retorna quando uma transação PENDING_REFERENCE está devida.
func (t *WagerTransaction) NextAttemptAt() time.Time { return t.nextAttemptAt }

// ReferenceExpiresAt retorna o prazo TTL para a referência.
func (t *WagerTransaction) ReferenceExpiresAt() time.Time { return t.referenceExpires }

// CorrelationID retorna o correlation id da requisição originadora.
func (t *WagerTransaction) CorrelationID() string { return t.correlationID }

// CreatedAt retorna o instante de criação.
func (t *WagerTransaction) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt retorna o instante da última mudança.
func (t *WagerTransaction) UpdatedAt() time.Time { return t.updatedAt }

// ProcessedAt retorna quando um estado terminal foi atingido (zero caso contrário).
func (t *WagerTransaction) ProcessedAt() time.Time { return t.processedAt }

// SameRequest informa se req tem o mesmo payload de negócio.
func (t *WagerTransaction) SameRequest(req ExternalRequest) bool {
	return t.payloadHash == req.payloadHash
}
