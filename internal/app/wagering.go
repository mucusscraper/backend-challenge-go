package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// maxConflictRetries limita os retries imediatos de corridas de concorrência.
const maxConflictRetries = 5

// WageringService implementa os casos de uso de transação de aposta.
type WageringService struct {
	uow     UnitOfWork
	clock   Clock
	newID   domain.IDGenerator
	policy  domain.ReferencePolicy
	log     *slog.Logger
	metrics *observability.Metrics
}

// NewWageringService constrói o serviço.
func NewWageringService(uow UnitOfWork, clock Clock, newID domain.IDGenerator, policy domain.ReferencePolicy,
	log *slog.Logger, metrics *observability.Metrics) *WageringService {
	return &WageringService{uow: uow, clock: clock, newID: newID, policy: policy, log: log, metrics: metrics}
}

// InboundMessage identifica a mensagem SQS sendo tratada, para o inbox.
type InboundMessage struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
}

// SubmitCommand é a entrada de Submit.
type SubmitCommand struct {
	Request       domain.ExternalRequest
	CorrelationID string
	// Source rotula métricas/logs: "http" ou "sqs".
	Source string
	// Message é definido para entregas SQS; o registro do inbox é então gravado
	// na mesma transação SQL que os efeitos financeiros.
	Message *InboundMessage
}

// SubmitResult é o resultado de Submit.
type SubmitResult struct {
	Transaction *domain.WagerTransaction
	// Replay é true quando a operação já havia sido registrada e seu
	// resultado persistido é retornado sem reaplicá-la.
	Replay bool
	// DuplicateMessage é true quando o inbox reconheceu o messageId.
	DuplicateMessage bool
}

// Submit registra e, quando possível, liquida uma operação externa em uma
// única transação SQL:
//
//  1. verificação do inbox (somente SQS): um messageId conhecido retorna o resultado armazenado;
//  2. verificação de idempotência por (provider, key) / (provider, externalId);
//  3. SELECT ... FOR UPDATE na carteira (serialização por carteira);
//  4. reverificação de idempotência sob o lock (duplicatas concorrentes esperam
//     no lock e então veem a linha commitada);
//  5. resolução de referência e liquidação de domínio;
//  6. inserir transação, atualizar carteira (version compare-and-set), inserir
//     entrada do ledger, adicionar eventos ao outbox, inserir registro inbox; COMMIT.
//
// Corridas de constraint única e deadlocks fazem rollback e retentam o unit inteiro;
// o retry então observa a linha do vencedor e vira um replay.
func (s *WageringService) Submit(ctx context.Context, cmd SubmitCommand) (SubmitResult, error) {
	start := time.Now()
	defer s.metrics.ObserveDuration(cmd.Source, start)
	req := cmd.Request
	ctx = observability.WithLogFields(ctx,
		"providerId", req.ProviderID(), "walletId", req.WalletID().String(), "correlationId", cmd.CorrelationID)

	var res SubmitResult
	err := s.withRetry(ctx, "submit", func() error {
		res = SubmitResult{}
		return s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
			r, err := s.submitInTx(ctx, tx, cmd)
			res = r
			return err
		})
	})
	if err != nil {
		return SubmitResult{}, err
	}
	t := res.Transaction
	if res.Replay {
		mech := "idempotency"
		if res.DuplicateMessage {
			mech = "inbox"
		}
		s.metrics.DuplicatesTotal.WithLabelValues(cmd.Source, mech).Inc()
	} else {
		s.metrics.TransactionsTotal.WithLabelValues(string(t.Kind()), string(t.Status()), cmd.Source).Inc()
	}
	s.log.InfoContext(ctx, "wager transaction submitted",
		"transactionId", t.ID().String(), "kind", t.Kind(), "status", t.Status(),
		"failureCode", t.FailureCode(), "idempotentReplay", res.Replay, "source", cmd.Source)
	return res, nil
}

func (s *WageringService) submitInTx(ctx context.Context, tx Tx, cmd SubmitCommand) (SubmitResult, error) {
	req := cmd.Request
	if m := cmd.Message; m != nil {
		rec, err := tx.Inbox().Get(ctx, m.Consumer, m.MessageID)
		if err != nil {
			return SubmitResult{}, err
		}
		if rec != nil {
			if rec.PayloadHash != m.PayloadHash {
				return SubmitResult{}, ErrMessageConflict
			}
			t, err := tx.Transactions().Get(ctx, rec.TransactionID)
			if err != nil {
				return SubmitResult{}, err
			}
			return SubmitResult{Transaction: t, Replay: true, DuplicateMessage: true}, nil
		}
	}

	// Caminho rápido: replay sem adquirir o lock da carteira.
	if t, err := s.findExisting(ctx, tx, req); err != nil || t != nil {
		if err != nil {
			return SubmitResult{}, err
		}
		return s.replay(ctx, tx, cmd, t)
	}

	w, err := tx.Wallets().GetForUpdate(ctx, req.WalletID())
	if err != nil {
		return SubmitResult{}, err
	}
	// Reverificação sob o lock: uma duplicata concorrente pode ter commitado
	// enquanto aguardávamos.
	if t, err := s.findExisting(ctx, tx, req); err != nil || t != nil {
		if err != nil {
			return SubmitResult{}, err
		}
		return s.replay(ctx, tx, cmd, t)
	}

	now := s.clock()
	t, err := domain.NewExternalTransaction(s.newID(), req, cmd.CorrelationID, now)
	if err != nil {
		return SubmitResult{}, err
	}
	ref, err := s.lookupReference(ctx, tx, t)
	if err != nil {
		return SubmitResult{}, err
	}
	expectedVersion := w.Version()
	settlement, err := domain.Settle(t, w, ref, s.settleContext(now, cmd))
	if err != nil {
		return SubmitResult{}, err
	}
	if err := tx.Transactions().Insert(ctx, t); err != nil {
		return SubmitResult{}, err
	}
	if err := s.persistSettlement(ctx, tx, w, expectedVersion, settlement); err != nil {
		return SubmitResult{}, err
	}
	if err := s.recordInbox(ctx, tx, cmd, t); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: t}, nil
}

func (s *WageringService) settleContext(now time.Time, cmd SubmitCommand) domain.SettleContext {
	causation := ""
	if cmd.Message != nil {
		causation = cmd.Message.MessageID
	}
	return domain.SettleContext{Now: now, NewID: s.newID, CausationID: causation, Policy: s.policy}
}

// findExisting aplica as regras de idempotência:
//   - mesma chave e mesmo hash de payload      -> replay;
//   - mesma chave e hash de payload diferente  -> ErrIdempotencyConflict;
//   - mesmo id externo com outra chave         -> ErrExternalIDConflict.
func (s *WageringService) findExisting(ctx context.Context, tx Tx, req domain.ExternalRequest) (*domain.WagerTransaction, error) {
	rows, err := tx.Transactions().FindByIdempotency(ctx, req.ProviderID(), req.IdempotencyKey(), req.ExternalTransactionID())
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	for _, t := range rows {
		if t.IdempotencyKey() == req.IdempotencyKey() {
			if !t.SameRequest(req) {
				return nil, ErrIdempotencyConflict
			}
			return t, nil
		}
	}
	return nil, ErrExternalIDConflict
}

func (s *WageringService) replay(ctx context.Context, tx Tx, cmd SubmitCommand, t *domain.WagerTransaction) (SubmitResult, error) {
	if err := s.recordInbox(ctx, tx, cmd, t); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: t, Replay: true}, nil
}

func (s *WageringService) recordInbox(ctx context.Context, tx Tx, cmd SubmitCommand, t *domain.WagerTransaction) error {
	m := cmd.Message
	if m == nil {
		return nil
	}
	return tx.Inbox().Insert(ctx, InboxRecord{
		Consumer:      m.Consumer,
		MessageID:     m.MessageID,
		PayloadHash:   m.PayloadHash,
		TransactionID: t.ID(),
		Outcome:       string(t.Status()),
		ReceivedAt:    m.ReceivedAt,
		CompletedAt:   s.clock(),
	})
}

// lookupReference resolve (providerId, referenceExternalTransactionId).
func (s *WageringService) lookupReference(ctx context.Context, tx Tx, t *domain.WagerTransaction) (domain.ReferenceState, error) {
	if t.ReferenceExternalTransactionID() == "" {
		return domain.ReferenceState{}, nil
	}
	ref, err := tx.Transactions().FindByExternalID(ctx, t.ProviderID(), t.ReferenceExternalTransactionID())
	if err != nil || ref == nil {
		return domain.ReferenceState{}, err
	}
	state := domain.ReferenceState{Transaction: ref}
	if t.Kind().IsReversal() {
		reversed, err := tx.Transactions().HasSuccessfulReversal(ctx, ref.ID())
		if err != nil {
			return domain.ReferenceState{}, err
		}
		state.AlreadyReversed = reversed
	}
	return state, nil
}

// persistSettlement grava a mudança da carteira, a entrada do ledger e os eventos.
func (s *WageringService) persistSettlement(ctx context.Context, tx Tx, w *domain.Wallet, expectedVersion int64, st domain.Settlement) error {
	if st.Entry != nil {
		if err := tx.Wallets().UpdateBalance(ctx, w, expectedVersion); err != nil {
			return err
		}
		if err := tx.Ledger().Insert(ctx, *st.Entry); err != nil {
			return err
		}
	}
	if len(st.Events) > 0 {
		return tx.Outbox().Append(ctx, st.Events...)
	}
	return nil
}

// withRetry retenta um unit of work em corridas de concorrência com um pequeno
// backoff com jitter. Outros erros são retornados imediatamente.
func (s *WageringService) withRetry(ctx context.Context, op string, fn func() error) error {
	return retryConflicts(ctx, s.metrics, s.log, op, fn)
}

func retryConflicts(ctx context.Context, m *observability.Metrics, log *slog.Logger, op string, fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxConflictRetries; attempt++ {
		err = fn()
		if err == nil || !IsRetryable(err) {
			return err
		}
		m.ConcurrencyConflictsTotal.WithLabelValues(op).Inc()
		m.RetriesTotal.WithLabelValues("db_tx").Inc()
		log.DebugContext(ctx, "retrying after concurrency conflict", "op", op, "attempt", attempt, "error", err)
		delay := time.Duration(attempt*attempt)*5*time.Millisecond + time.Duration(rand.IntN(5))*time.Millisecond
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", ErrTransient, ctx.Err())
		case <-time.After(delay):
		}
	}
	// Contenção persistente é exposta como condição transitória.
	return fmt.Errorf("%w: %v", ErrTransient, err)
}

// GetTransaction retorna uma transação pelo id interno.
func (s *WageringService) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	var t *domain.WagerTransaction
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		t, err = tx.Transactions().Get(ctx, id)
		return err
	})
	return t, err
}

// GetByExternalID retorna a transação de um provedor pelo id externo.
func (s *WageringService) GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	var t *domain.WagerTransaction
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		t, err = tx.Transactions().FindByExternalID(ctx, providerID, externalID)
		if err == nil && t == nil {
			err = ErrNotFound
		}
		return err
	})
	return t, err
}

// ResumeDue retoma até limit transações PENDING/PENDING_REFERENCE
// cuja próxima tentativa está devida. É seguro rodar em qualquer número de
// instâncias: cada transação é revalidada sob o lock de sua carteira, e
// carteiras bloqueadas por outra instância são ignoradas (SKIP LOCKED) e
// retomadas em um poll posterior. Retorna quantas transações progrediram.
func (s *WageringService) ResumeDue(ctx context.Context, limit int) (int, error) {
	var due []DueTransaction
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		due, err = tx.Transactions().ListDue(ctx, s.clock(), limit)
		return err
	})
	if err != nil {
		return 0, err
	}
	progressed := 0
	for _, d := range due {
		if ctx.Err() != nil {
			return progressed, ctx.Err()
		}
		done, err := s.resumeOne(ctx, d)
		if done {
			progressed++
		}
		if err != nil {
			if IsTransient(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.log.WarnContext(ctx, "pending transaction will be retried", "transactionId", d.ID.String(), "error", err)
				continue
			}
			s.failPermanently(ctx, d, err)
		}
	}
	return progressed, nil
}

func (s *WageringService) resumeOne(ctx context.Context, d DueTransaction) (bool, error) {
	ctx = observability.WithLogFields(ctx, "transactionId", d.ID.String(), "walletId", d.WalletID.String())
	var outcome *domain.WagerTransaction
	err := s.withRetry(ctx, "resume", func() error {
		outcome = nil
		return s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
			w, err := tx.Wallets().TryGetForUpdate(ctx, d.WalletID)
			if err != nil || w == nil {
				return err // carteira nil: bloqueada em outro lugar, retenta no próximo poll
			}
			t, err := tx.Transactions().Get(ctx, d.ID)
			if err != nil {
				return err
			}
			now := s.clock()
			if t.Status().IsTerminal() || t.NextAttemptAt().After(now) {
				return nil // já tratada por outra instância
			}
			prev := t.Status()
			ref, err := s.lookupReference(ctx, tx, t)
			if err != nil {
				return err
			}
			expectedVersion := w.Version()
			st, err := domain.Settle(t, w, ref, domain.SettleContext{Now: now, NewID: s.newID, Policy: s.policy})
			if err != nil {
				return err
			}
			if err := tx.Transactions().Update(ctx, t, prev); err != nil {
				return err
			}
			if err := s.persistSettlement(ctx, tx, w, expectedVersion, st); err != nil {
				return err
			}
			outcome = t
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	if outcome != nil {
		s.metrics.RetriesTotal.WithLabelValues("pending").Inc()
		if outcome.Status().IsTerminal() {
			s.metrics.PendingResolutions.WithLabelValues(string(outcome.Status())).Inc()
			s.metrics.TransactionsTotal.WithLabelValues(string(outcome.Kind()), string(outcome.Status()), "worker").Inc()
		}
		s.log.InfoContext(ctx, "pending transaction resumed", "status", outcome.Status(),
			"failureCode", outcome.FailureCode(), "attempts", outcome.Attempts())
	}
	return outcome != nil, nil
}

// failPermanently registra o status FAILED para auditoria quando a retomada
// encontrou um erro não transitório (dados corrompidos, constraint violada...).
func (s *WageringService) failPermanently(ctx context.Context, d DueTransaction, cause error) {
	s.log.ErrorContext(ctx, "pending transaction failed permanently", "transactionId", d.ID.String(), "error", cause)
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		if _, err := tx.Wallets().GetForUpdate(ctx, d.WalletID); err != nil {
			return err
		}
		t, err := tx.Transactions().Get(ctx, d.ID)
		if err != nil || t.Status().IsTerminal() {
			return err
		}
		prev := t.Status()
		if err := domain.FailPermanently(t, s.clock()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t, prev)
	})
	if err != nil {
		s.log.ErrorContext(ctx, "could not record FAILED status", "transactionId", d.ID.String(), "error", err)
		return
	}
	s.metrics.PendingResolutions.WithLabelValues(string(domain.StatusFailed)).Inc()
}
