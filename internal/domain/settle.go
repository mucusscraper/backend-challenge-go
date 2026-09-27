package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// IDGenerator produz novos identificadores únicos (UUIDv7 em produção).
type IDGenerator func() uuid.UUID

// ReferencePolicy limita por quanto tempo uma transação pode esperar por uma
// referência que ainda não chegou (ou que ainda está pendente).
type ReferencePolicy struct {
	// MaxAttempts é o número máximo de tentativas de resolução, contando a
	// primeira síncrona.
	MaxAttempts int
	// TTL é o tempo máximo desde a criação da transação.
	TTL time.Duration
	// BaseBackoff é o atraso após a primeira tentativa; dobra a cada tentativa
	// (backoff exponencial) até MaxBackoff.
	BaseBackoff time.Duration
	// MaxBackoff limita o atraso entre tentativas.
	MaxBackoff time.Duration
}

// DefaultReferencePolicy é usada quando nenhuma política está configurada.
var DefaultReferencePolicy = ReferencePolicy{
	MaxAttempts: 10,
	TTL:         10 * time.Minute,
	BaseBackoff: 500 * time.Millisecond,
	MaxBackoff:  60 * time.Second,
}

// Delay retorna o backoff antes da tentativa número attempt+1 (attempt >= 1).
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

// ReferenceState é o que a camada de aplicação encontrou ao resolver
// (providerId, referenceExternalTransactionId).
type ReferenceState struct {
	// Transaction é a transação referenciada, nil quando não encontrada.
	Transaction *WagerTransaction
	// AlreadyReversed é true quando um REFUND ou ROLLBACK PROCESSED já tem
	// como alvo a referência. O banco aplica a mesma regra com um índice único
	// parcial, de modo que uma corrida não pode produzir duas reversões.
	AlreadyReversed bool
}

// SettleContext carrega as entradas não determinísticas de Settle.
type SettleContext struct {
	Now         time.Time
	NewID       IDGenerator
	CausationID string
	Policy      ReferencePolicy
}

// Settlement é o resultado de Settle: a entrada do ledger a adicionar (se o
// saldo mudou) e os eventos a armazenar no outbox. A transação e a carteira
// passadas para Settle são mutadas no lugar.
type Settlement struct {
	Entry  *LedgerEntry
	Events []Event
}

// Settle aplica uma transação aberta (PENDING ou PENDING_REFERENCE) à sua
// carteira. Codifica todas as regras de negócio dos cinco tipos externos:
//
//   - BET: débito, valor positivo, saldo suficiente (senão INSUFFICIENT_FUNDS);
//   - WIN: crédito; uma referência opcional deve ser um BET da mesma rodada;
//   - LOSS: sem movimento, sem entrada no ledger, sem mudança de versão;
//   - REFUND: crédito do valor total de um BET PROCESSED;
//   - ROLLBACK: movimento oposto de um BET, WIN ou REFUND PROCESSED, valor
//     total; um débito que tornaria o saldo negativo é rejeitado com
//     REVERSAL_INSUFFICIENT_FUNDS.
//
// Uma referência ausente (ou ainda pendente) move a transação para
// PENDING_REFERENCE com backoff exponencial até a política expirar, então
// a transação é REJECTED.
//
// Recusas de negócio não são retornadas como erros: produzem uma transação
// REJECTED mais um evento WagerTransactionRejected. Um erro é retornado
// apenas para chamadas inválidas ou invariantes quebrados.
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
			return s.out, nil // pendente
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

// movementFor retorna a direção do movimento da carteira para um tipo, e
// false quando o tipo não movimenta dinheiro (LOSS).
func movementFor(kind Kind, ref *WagerTransaction) (Direction, bool) {
	switch kind {
	case KindBet:
		return DirectionDebit, true
	case KindWin, KindRefund:
		return DirectionCredit, true
	case KindRollback:
		d, _ := movementFor(ref.kind, nil)
		return d.Opposite(), true
	default: // LOSS — sem movimento
		return "", false
	}
}

// allowedReferenceKinds lista o que cada tipo pode referenciar.
var allowedReferenceKinds = map[Kind][]Kind{
	KindWin:      {KindBet},
	KindRefund:   {KindBet},
	KindRollback: {KindBet, KindWin, KindRefund},
}

// checkReference valida a referência. Retorna ready=true quando a operação
// pode prosseguir, um failure code para uma rejeição definitiva, ou nenhum
// dos dois quando a transação foi movida para PENDING_REFERENCE.
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

// waitForReference reagenda a transação ou, quando o orçamento de retry ou
// o TTL se esgota, a rejeita com expiredCode.
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

// FailPermanently marca uma transação aberta como FAILED (falha de
// infraestrutura registrada para auditoria). Nenhum evento é emitido: FAILED
// é um estado de auditoria interno, não um resultado de negócio.
func FailPermanently(t *WagerTransaction, now time.Time) error {
	return t.MarkFailed(FailureProcessingFailed, now)
}

// OpenWalletParams agrupa as entradas de OpenWallet.
type OpenWalletParams struct {
	WalletID       uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
	Now            time.Time
	NewID          IDGenerator
}

// Opening é o resultado de abertura de uma carteira. Para um saldo inicial
// positivo contém a transação OPENING interna (já PROCESSED), sua entrada
// CREDIT no ledger (versão 1, 0.00 -> inicial) e os eventos
// WagerTransactionProcessed + WalletBalanceChanged. Para saldo inicial zero
// apenas a carteira é criada.
type Opening struct {
	Wallet      *Wallet
	Transaction *WagerTransaction
	Entry       *LedgerEntry
	Events      []Event
}

// OpenWallet cria uma carteira (versão 1) e, quando o saldo inicial é
// positivo, tudo que deve ser commitado junto com ela.
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
