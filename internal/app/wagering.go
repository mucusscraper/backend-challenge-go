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

// maxConflictRetries bounds immediate retries of concurrency races.
const maxConflictRetries = 5

// WageringService implements the wager transaction use cases.
type WageringService struct {
	uow     UnitOfWork
	clock   Clock
	newID   domain.IDGenerator
	policy  domain.ReferencePolicy
	log     *slog.Logger
	metrics *observability.Metrics
}

// NewWageringService builds the service.
func NewWageringService(uow UnitOfWork, clock Clock, newID domain.IDGenerator, policy domain.ReferencePolicy,
	log *slog.Logger, metrics *observability.Metrics) *WageringService {
	return &WageringService{uow: uow, clock: clock, newID: newID, policy: policy, log: log, metrics: metrics}
}

// InboundMessage identifies the SQS message being handled, for the inbox.
type InboundMessage struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
}

// SubmitCommand is the input of Submit.
type SubmitCommand struct {
	Request       domain.ExternalRequest
	CorrelationID string
	// Source labels metrics/logs: "http" or "sqs".
	Source string
	// Message is set for SQS deliveries; the inbox record is then written in
	// the same SQL transaction as the financial effects.
	Message *InboundMessage
}

// SubmitResult is the outcome of Submit.
type SubmitResult struct {
	Transaction *domain.WagerTransaction
	// Replay is true when the operation had already been registered and its
	// persisted result is returned without reapplying it.
	Replay bool
	// DuplicateMessage is true when the inbox recognised the messageId.
	DuplicateMessage bool
}

// Submit registers and, when possible, settles an external operation in a
// single SQL transaction:
//
//  1. inbox check (SQS only): a known messageId returns the stored result;
//  2. idempotency check by (provider, key) / (provider, externalId);
//  3. SELECT ... FOR UPDATE on the wallet (per-wallet serialisation);
//  4. idempotency re-check under the lock (concurrent duplicates wait on
//     the lock and then see the committed row);
//  5. reference resolution and domain settlement;
//  6. insert transaction, update wallet (version compare-and-set), insert
//     ledger entry, append outbox events, insert inbox record; COMMIT.
//
// Unique-constraint races and deadlocks roll back and retry the whole unit;
// the retry then observes the winner's row and becomes a replay.
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

	// Fast path: replay without taking the wallet lock.
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
	// Re-check under the lock: a concurrent duplicate may have committed
	// while we were waiting.
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

// findExisting applies the idempotency rules:
//   - same key and same payload hash      -> replay;
//   - same key and different payload hash -> ErrIdempotencyConflict;
//   - same external id with another key   -> ErrExternalIDConflict.
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

// lookupReference resolves (providerId, referenceExternalTransactionId).
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

// persistSettlement writes the wallet change, ledger entry and events.
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

// withRetry retries a unit of work on concurrency races with a small
// jittered backoff. Other errors are returned immediately.
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
	// Persistent contention is surfaced as a transient condition.
	return fmt.Errorf("%w: %v", ErrTransient, err)
}

// GetTransaction returns a transaction by internal id.
func (s *WageringService) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	var t *domain.WagerTransaction
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		t, err = tx.Transactions().Get(ctx, id)
		return err
	})
	return t, err
}

// GetByExternalID returns a provider's transaction by external id.
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

// ResumeDue resumes up to limit PENDING/PENDING_REFERENCE transactions
// whose next attempt is due. It is safe to run on any number of instances:
// each transaction is re-validated under its wallet lock, and wallets locked
// by someone else are skipped (SKIP LOCKED) and picked up on a later poll.
// It returns how many transactions actually progressed.
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
				return err // nil wallet: locked elsewhere, retry on next poll
			}
			t, err := tx.Transactions().Get(ctx, d.ID)
			if err != nil {
				return err
			}
			now := s.clock()
			if t.Status().IsTerminal() || t.NextAttemptAt().After(now) {
				return nil // already handled by another instance
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

// failPermanently records a FAILED status for audit when resuming hit a
// non-transient error (corrupt data, violated constraint...).
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
