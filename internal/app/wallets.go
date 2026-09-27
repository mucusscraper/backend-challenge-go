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

// Limites de paginação do ledger.
const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// ErrInvalidCursor indica um cursor de paginação malformado.
var ErrInvalidCursor = errors.New("invalid cursor")

// WalletService implementa os casos de uso de carteira (somente serviço interno).
type WalletService struct {
	uow     UnitOfWork
	clock   Clock
	newID   domain.IDGenerator
	log     *slog.Logger
	metrics *observability.Metrics
}

// NewWalletService constrói o serviço.
func NewWalletService(uow UnitOfWork, clock Clock, newID domain.IDGenerator, log *slog.Logger,
	metrics *observability.Metrics) *WalletService {
	return &WalletService{uow: uow, clock: clock, newID: newID, log: log, metrics: metrics}
}

// OpenWallet cria uma carteira. Para um saldo inicial positivo, a transação
// OPENING (PROCESSED), sua entrada CREDIT no ledger e os eventos do outbox
// WagerTransactionProcessed + WalletBalanceChanged são commitados juntos com
// a carteira. Uma segunda carteira para o mesmo (player, currency) retorna
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

// GetWallet retorna uma carteira.
func (s *WalletService) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	var w *domain.Wallet
	err := s.uow.Run(ctx, func(ctx context.Context, tx Tx) error {
		var err error
		w, err = tx.Wallets().Get(ctx, id)
		return err
	})
	return w, err
}

// LedgerPage é uma página de entradas do ledger.
type LedgerPage struct {
	Entries []domain.LedgerEntry
	// NextCursor é vazio na última página.
	NextCursor string
}

// Ledger lista entradas em ordem estável (seq) usando um cursor opaco.
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

// EncodeCursor cria um cursor opaco a partir de uma sequência do ledger.
func EncodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("v1:" + strconv.FormatInt(seq, 10)))
}

// DecodeCursor analisa um cursor produzido por EncodeCursor ("" = início).
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

// Reconciliation é o resultado da comparação do saldo armazenado de uma
// carteira com o saldo reconstruído a partir do seu ledger.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	// Difference = armazenado - calculado.
	Difference     money.Money
	Consistent     bool
	CheckedEntries int64
}

// Reconcile reconstrói o saldo a partir do ledger (OPENING incluído) dentro
// de um snapshot REPEATABLE READ somente leitura, de modo que a linha da
// carteira e o ledger são observados no mesmo instante. Nunca modifica o saldo;
// uma divergência é reportada no resultado, registrada em log e contada em
// uma métrica.
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
