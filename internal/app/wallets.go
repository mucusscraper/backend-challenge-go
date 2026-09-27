package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// Ledger pagination limits.
const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// ErrInvalidCursor reports a malformed pagination cursor.
var ErrInvalidCursor = errors.New("invalid cursor")

// WalletService implements the wallet use cases (internal service only).
type WalletService struct {
	uow     UnitOfWork
	clock   Clock
	newID   domain.IDGenerator
	log     *slog.Logger
	metrics *observability.Metrics
}

// NewWalletService builds the service.
func NewWalletService(uow UnitOfWork, clock Clock, newID domain.IDGenerator, log *slog.Logger,
	metrics *observability.Metrics) *WalletService {
	return &WalletService{uow: uow, clock: clock, newID: newID, log: log, metrics: metrics}
}

// OpenWallet creates a wallet. For a positive initial balance the OPENING
// transaction (PROCESSED), its CREDIT ledger entry and the outbox events
// WagerTransactionProcessed + WalletBalanceChanged are committed together
// with the wallet. A second wallet for the same (player, currency) returns
// ErrWalletAlreadyExists.
func (s *WalletService) OpenWallet(ctx context.Context, playerID uuid.UUID, initial money.Money, correlationID string) (*domain.Wallet, error) {
	opening, err := domain.OpenWallet(domain.OpenWalletParams{
		WalletID:       s.newID(),
		PlayerID:       playerID,
		InitialBalance: initial,
		CorrelationID:  correlationID,
		Now:            s.clock(),
		NewID:          s.newID,
	})
	if err != nil {
		return nil, err
	}
	err = s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		if err := tx.Wallets().Insert(ctx, opening.Wallet); err != nil {
			return err
		}
		if opening.Transaction == nil {
			return nil
		}
		if err := tx.Transactions().Insert(ctx, opening.Transaction); err != nil {
			return err
		}
		if err := tx.Ledger().Insert(ctx, *opening.Entry); err != nil {
			return err
		}
		return tx.Outbox().Append(ctx, opening.Events...)
	})
	if err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "wallet opened", "walletId", opening.Wallet.ID().String(), "correlationId", correlationID)
	if opening.Transaction != nil {
		s.metrics.TransactionsTotal.WithLabelValues(string(domain.KindOpening), string(domain.StatusProcessed), "http").Inc()
	}
	return opening.Wallet, nil
}

// GetWallet returns a wallet.
func (s *WalletService) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	var w *domain.Wallet
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		w, err = tx.Wallets().Get(ctx, id)
		return err
	})
	return w, err
}

// LedgerPage is one page of ledger entries.
type LedgerPage struct {
	Entries []domain.LedgerEntry
	// NextCursor is empty on the last page.
	NextCursor string
}

// Ledger lists entries in stable (seq) order using an opaque cursor.
func (s *WalletService) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := DecodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		limit = MaxLedgerLimit
	}
	var page LedgerPage
	err = s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		if _, err := tx.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		rows, err := tx.Ledger().List(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		hasMore := len(rows) > limit
		if hasMore {
			rows = rows[:limit]
		}
		page.Entries = make([]domain.LedgerEntry, len(rows))
		for i, r := range rows {
			page.Entries[i] = r.Entry
		}
		if hasMore {
			page.NextCursor = EncodeCursor(rows[len(rows)-1].Seq)
		}
		return nil
	})
	return page, err
}

// EncodeCursor makes an opaque cursor from a ledger sequence.
func EncodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("v1:" + strconv.FormatInt(seq, 10)))
}

// DecodeCursor parses a cursor produced by EncodeCursor ("" = start).
func DecodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	v, ok := strings.CutPrefix(string(raw), "v1:")
	if !ok {
		return 0, ErrInvalidCursor
	}
	seq, err := strconv.ParseInt(v, 10, 64)
	if err != nil || seq < 0 {
		return 0, ErrInvalidCursor
	}
	return seq, nil
}

// Reconciliation is the result of comparing a wallet's stored balance with
// the balance rebuilt from its ledger.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	// Difference = stored - calculated.
	Difference     money.Money
	Consistent     bool
	CheckedEntries int64
}

// Reconcile rebuilds the balance from the ledger (OPENING included) inside
// a REPEATABLE READ read-only snapshot, so the wallet row and the ledger
// are observed at the same instant. It never modifies the balance; a
// divergence is reported in the result, logged and counted in a metric.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var rec Reconciliation
	err := s.uow.Snapshot(ctx, func(ctx context.Context, tx Tx) error {
		w, err := tx.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := tx.Ledger().Totals(ctx, walletID)
		if err != nil {
			return err
		}
		credits, err := money.FromUnits(totals.CreditsMinor, w.Currency())
		if err != nil {
			return err
		}
		debits, err := money.FromUnits(totals.DebitsMinor, w.Currency())
		if err != nil {
			return err
		}
		calculated, err := credits.Sub(debits)
		if err != nil {
			return fmt.Errorf("reconciliation arithmetic: %w", err)
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return fmt.Errorf("reconciliation arithmetic: %w", err)
		}
		rec = Reconciliation{
			WalletID:          walletID,
			StoredBalance:     w.Balance(),
			CalculatedBalance: calculated,
			Difference:        diff,
			Consistent:        diff.IsZero(),
			CheckedEntries:    totals.Count,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !rec.Consistent {
		s.metrics.ReconciliationDivergences.Inc()
		s.log.ErrorContext(ctx, "reconciliation divergence", "walletId", walletID.String(),
			"stored", rec.StoredBalance.Amount(), "calculated", rec.CalculatedBalance.Amount(),
			"difference", rec.Difference.Amount())
	} else {
		s.log.InfoContext(ctx, "reconciliation consistent", "walletId", walletID.String(), "entries", rec.CheckedEntries)
	}
	return rec, nil
}
