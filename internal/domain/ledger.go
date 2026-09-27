package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// LedgerEntry é uma linha imutável do ledger append-only de uma carteira.
// Toda mudança de saldo tem exatamente uma entrada, persistida na mesma
// transação SQL que o novo saldo. Correções são feitas com novas entradas
// (ex.: um ROLLBACK), nunca editando as existentes; o banco aplica isso com
// triggers.
//
// Todos os campos são não exportados e não há setters: uma vez construída,
// uma entrada não pode mudar.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// LedgerEntryParams agrupa os atributos de uma entrada do ledger.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	// WalletVersion é a versão da carteira produzida por esta entrada. Permite
	// que o banco detecte forks/lost updates via UNIQUE(wallet_id, version).
	WalletVersion int64
	CreatedAt     time.Time
}

// NewLedgerEntry valida e constrói uma entrada. Aplica:
//   - identificadores não nulos, uma direção conhecida e um valor positivo;
//   - uma única moeda para amount, balanceBefore e balanceAfter;
//   - saldos não negativos;
//   - balanceAfter = balanceBefore + amount (CREDIT) ou - amount (DEBIT).
//
// A mesma função é usada para reidratação, pois uma entrada não tem
// comportamento a repetir: a validação é tudo.
func NewLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return LedgerEntry{}, invalidArg("ledger entry identifiers are required")
	}
	if p.Direction != DirectionDebit && p.Direction != DirectionCredit {
		return LedgerEntry{}, invalidArg("ledger entry direction %q", p.Direction)
	}
	if !p.Amount.IsValid() || !p.BalanceBefore.IsValid() || !p.BalanceAfter.IsValid() {
		return LedgerEntry{}, invalidArg("ledger entry money must be initialised")
	}
	if !p.Amount.IsPositive() {
		return LedgerEntry{}, invalidArg("ledger entry amount must be positive")
	}
	if p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return LedgerEntry{}, invalidArg("ledger balances cannot be negative")
	}
	if p.WalletVersion < 1 {
		return LedgerEntry{}, invalidArg("ledger wallet version must be >= 1")
	}
	if p.CreatedAt.IsZero() {
		return LedgerEntry{}, invalidArg("ledger entry createdAt is required")
	}
	var expected money.Money
	var err error
	if p.Direction == DirectionCredit {
		expected, err = p.BalanceBefore.Add(p.Amount)
	} else {
		expected, err = p.BalanceBefore.Sub(p.Amount)
	}
	if err != nil {
		return LedgerEntry{}, invalidArg("ledger arithmetic: %v", err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return LedgerEntry{}, invalidArg("ledger arithmetic mismatch: %s %s %s != %s",
			p.BalanceBefore, p.Direction, p.Amount, p.BalanceAfter)
	}
	return LedgerEntry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		walletVersion: p.WalletVersion,
		createdAt:     p.CreatedAt.UTC(),
	}, nil
}

// ID retorna o identificador da entrada.
func (e LedgerEntry) ID() uuid.UUID { return e.id }

// WalletID retorna a carteira à qual a entrada pertence.
func (e LedgerEntry) WalletID() uuid.UUID { return e.walletID }

// TransactionID retorna a transação que produziu a entrada.
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }

// Direction retorna DEBIT ou CREDIT.
func (e LedgerEntry) Direction() Direction { return e.direction }

// Amount retorna o valor (positivo) movimentado.
func (e LedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore retorna o saldo da carteira antes da entrada.
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter retorna o saldo da carteira após a entrada.
func (e LedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// WalletVersion retorna a versão da carteira produzida pela entrada.
func (e LedgerEntry) WalletVersion() int64 { return e.walletVersion }

// CreatedAt retorna o timestamp da entrada (UTC).
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }

// SignedAmount retorna +amount para créditos e -amount para débitos; é
// usado pela reconciliação para reconstruir o saldo.
func (e LedgerEntry) SignedAmount() (money.Money, error) {
	if e.direction == DirectionCredit {
		return e.amount, nil
	}
	return e.amount.Neg()
}
