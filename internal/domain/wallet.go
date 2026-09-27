package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Wallet é a raiz do agregado financeiro. Seu saldo só pode mudar por meio
// de Debit e Credit, que sempre retornam a LedgerEntry correspondente; a
// camada de aplicação persiste ambos na mesma transação SQL.
//
// Concorrência: o agregado carrega uma versão. Começa em 1 quando a
// carteira é aberta e é incrementada exatamente uma vez por mudança de saldo. O
// repositório atualiza a linha com "WHERE version = <versão carregada>" enquanto
// mantém um row lock (SELECT ... FOR UPDATE), de modo que um escritor concorrente
// nunca pode sobrescrever uma mudança commitada (sem lost update).
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// InitialWalletVersion é a versão de uma carteira recém-aberta.
const InitialWalletVersion int64 = 1

// WalletSnapshot é o estado completo persistido de uma carteira, usado para
// reidratá-la a partir do armazenamento.
type WalletSnapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RehydrateWallet reconstrói uma carteira a partir do estado persistido.
// Valida o snapshot mas não repete nenhum movimento nem emite eventos.
func RehydrateWallet(s WalletSnapshot) (*Wallet, error) {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, invalidArg("wallet identifiers are required")
	}
	if !s.Balance.IsValid() {
		return nil, invalidArg("wallet balance must be initialised")
	}
	if s.Balance.IsNegative() {
		return nil, invalidArg("wallet balance cannot be negative")
	}
	if s.Version < InitialWalletVersion {
		return nil, invalidArg("wallet version must be >= 1")
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, invalidArg("wallet timestamps are required")
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		currency:  s.Balance.Currency(),
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt.UTC(),
		updatedAt: s.UpdatedAt.UTC(),
	}, nil
}

// newWallet cria uma carteira nova na versão 1. Não é exportada porque
// carteiras são criadas apenas por OpenWallet, que também produz a transação
// OPENING, a entrada do ledger e os eventos quando necessário.
func newWallet(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, invalidArg("wallet identifiers are required")
	}
	if !initial.IsValid() {
		return nil, invalidArg("initial balance must be initialised")
	}
	if initial.IsNegative() {
		return nil, invalidArg("initial balance cannot be negative")
	}
	if now.IsZero() {
		return nil, invalidArg("creation time is required")
	}
	now = now.UTC()
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  initial.Currency(),
		balance:   initial,
		version:   InitialWalletVersion,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// ID retorna o identificador da carteira.
func (w *Wallet) ID() uuid.UUID { return w.id }

// PlayerID retorna o dono da carteira.
func (w *Wallet) PlayerID() uuid.UUID { return w.playerID }

// Currency retorna a moeda da carteira.
func (w *Wallet) Currency() money.Currency { return w.currency }

// Balance retorna o saldo atual.
func (w *Wallet) Balance() money.Money { return w.balance }

// Version retorna a versão para controle de concorrência otimista.
func (w *Wallet) Version() int64 { return w.version }

// CreatedAt retorna o instante de criação (UTC).
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt retorna o instante da última mudança de saldo (UTC).
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Movement descreve uma mudança de saldo solicitada na carteira.
type Movement struct {
	EntryID       uuid.UUID
	TransactionID uuid.UUID
	Amount        money.Money
	At            time.Time
}

func (w *Wallet) validateMovement(m Movement) error {
	if m.EntryID == uuid.Nil || m.TransactionID == uuid.Nil {
		return invalidArg("movement identifiers are required")
	}
	if !m.Amount.IsValid() {
		return invalidArg("movement amount must be initialised")
	}
	if m.Amount.Currency() != w.currency {
		return reject(FailureCurrencyMismatch, "wallet currency is %s, movement is %s", w.currency, m.Amount.Currency())
	}
	if !m.Amount.IsPositive() {
		return invalidArg("movement amount must be positive")
	}
	if m.At.IsZero() {
		return invalidArg("movement time is required")
	}
	return nil
}

// Debit remove o valor do saldo e retorna a entrada do ledger. Retorna um
// *RejectionError com FailureInsufficientFunds quando o saldo ficaria negativo;
// a carteira é deixada intocada em qualquer erro.
func (w *Wallet) Debit(m Movement) (LedgerEntry, error) {
	if err := w.validateMovement(m); err != nil {
		return LedgerEntry{}, err
	}
	cmp, err := w.balance.Cmp(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	if cmp < 0 {
		return LedgerEntry{}, reject(FailureInsufficientFunds, "balance %s is lower than %s", w.balance, m.Amount)
	}
	after, err := w.balance.Sub(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	return w.apply(DirectionDebit, m, after)
}

// Credit adiciona o valor ao saldo e retorna a entrada do ledger.
func (w *Wallet) Credit(m Movement) (LedgerEntry, error) {
	if err := w.validateMovement(m); err != nil {
		return LedgerEntry{}, err
	}
	after, err := w.balance.Add(m.Amount)
	if err != nil {
		return LedgerEntry{}, err
	}
	return w.apply(DirectionCredit, m, after)
}

// apply constrói a entrada primeiro e só muta a carteira quando a entrada
// é válida, de modo que uma falha nunca deixa um estado meio aplicado.
func (w *Wallet) apply(dir Direction, m Movement, after money.Money) (LedgerEntry, error) {
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            m.EntryID,
		WalletID:      w.id,
		TransactionID: m.TransactionID,
		Direction:     dir,
		Amount:        m.Amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		WalletVersion: w.version + 1,
		CreatedAt:     m.At,
	})
	if err != nil {
		return LedgerEntry{}, errors.Join(ErrInvariantViolation, err)
	}
	w.balance = after
	w.version++
	w.updatedAt = m.At.UTC()
	return entry, nil
}
