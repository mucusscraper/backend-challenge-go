package httpapi

import (
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// moneyDTO is the external money contract. Both fields are JSON strings;
// a JSON number for "amount" fails decoding, so no float is ever parsed.
type moneyDTO = money.DTO

// openWalletRequest is the body of POST /wallets.
type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

// walletResponse is returned by POST /wallets and GET /wallets/{id}.
type walletResponse struct {
	ID        string   `json:"id"`
	PlayerID  string   `json:"playerId"`
	Balance   moneyDTO `json:"balance"`
	Version   int64    `json:"version"`
	CreatedAt string   `json:"createdAt"`
	UpdatedAt string   `json:"updatedAt"`
}

func toWalletResponse(w *domain.Wallet) walletResponse {
	return walletResponse{
		ID:        w.ID().String(),
		PlayerID:  w.PlayerID().String(),
		Balance:   w.Balance().ToDTO(),
		Version:   w.Version(),
		CreatedAt: formatTime(w.CreatedAt()),
		UpdatedAt: formatTime(w.UpdatedAt()),
	}
}

// ledgerEntryResponse is one ledger line.
type ledgerEntryResponse struct {
	ID            string   `json:"id"`
	TransactionID string   `json:"transactionId"`
	Direction     string   `json:"direction"`
	Money         moneyDTO `json:"money"`
	BalanceBefore moneyDTO `json:"balanceBefore"`
	BalanceAfter  moneyDTO `json:"balanceAfter"`
	WalletVersion int64    `json:"walletVersion"`
	CreatedAt     string   `json:"createdAt"`
}

// ledgerResponse is returned by GET /wallets/{id}/ledger.
type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

func toLedgerResponse(walletID string, p app.LedgerPage) ledgerResponse {
	out := ledgerResponse{WalletID: walletID, Entries: make([]ledgerEntryResponse, len(p.Entries))}
	for i, e := range p.Entries {
		out.Entries[i] = ledgerEntryResponse{
			ID:            e.ID().String(),
			TransactionID: e.TransactionID().String(),
			Direction:     string(e.Direction()),
			Money:         e.Amount().ToDTO(),
			BalanceBefore: e.BalanceBefore().ToDTO(),
			BalanceAfter:  e.BalanceAfter().ToDTO(),
			WalletVersion: e.WalletVersion(),
			CreatedAt:     formatTime(e.CreatedAt()),
		}
	}
	if p.NextCursor != "" {
		c := p.NextCursor
		out.NextCursor = &c
	}
	return out
}

// submitRequest is the body of POST /wagering/transactions.
type submitRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId,omitempty"`
}

func (r submitRequest) toRaw(idempotencyKey string) domain.RawExternalRequest {
	return domain.RawExternalRequest{
		ProviderID:                     r.ProviderID,
		ExternalTransactionID:          r.ExternalTransactionID,
		IdempotencyKey:                 idempotencyKey,
		PlayerID:                       r.PlayerID,
		WalletID:                       r.WalletID,
		RoundID:                        r.RoundID,
		GameID:                         r.GameID,
		Kind:                           r.Kind,
		Amount:                         r.Money.Amount,
		Currency:                       r.Money.Currency,
		ReferenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}
}

// submitResponse is returned by POST /wagering/transactions. balance is the
// balance observed when the operation was concluded (not the current one),
// so replays return exactly the original result.
type submitResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      *string   `json:"failureCode,omitempty"`
	NextAttemptAt    *string   `json:"nextAttemptAt,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

func toSubmitResponse(t *domain.WagerTransaction, replay bool) submitResponse {
	out := submitResponse{
		TransactionID:    t.ID().String(),
		Status:           string(t.Status()),
		IdempotentReplay: replay,
	}
	if rb, ok := t.ResultBalance(); ok {
		d := rb.ToDTO()
		out.Balance = &d
	}
	if fc := t.FailureCode(); fc != "" {
		s := string(fc)
		out.FailureCode = &s
	}
	if !t.NextAttemptAt().IsZero() && !t.Status().IsTerminal() {
		s := formatTime(t.NextAttemptAt())
		out.NextAttemptAt = &s
	}
	return out
}

// transactionResponse is the full view returned by the GET endpoints.
type transactionResponse struct {
	TransactionID                  string    `json:"transactionId"`
	Origin                         string    `json:"origin"`
	Kind                           string    `json:"kind"`
	Status                         string    `json:"status"`
	ProviderID                     *string   `json:"providerId,omitempty"`
	ExternalTransactionID          *string   `json:"externalTransactionId,omitempty"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        *string   `json:"roundId,omitempty"`
	GameID                         *string   `json:"gameId,omitempty"`
	Money                          moneyDTO  `json:"money"`
	ReferenceExternalTransactionID *string   `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *string   `json:"referenceTransactionId,omitempty"`
	FailureCode                    *string   `json:"failureCode,omitempty"`
	Balance                        *moneyDTO `json:"balance,omitempty"`
	Attempts                       int       `json:"attempts"`
	NextAttemptAt                  *string   `json:"nextAttemptAt,omitempty"`
	ReferenceExpiresAt             *string   `json:"referenceExpiresAt,omitempty"`
	CreatedAt                      string    `json:"createdAt"`
	UpdatedAt                      string    `json:"updatedAt"`
	ProcessedAt                    *string   `json:"processedAt,omitempty"`
}

func toTransactionResponse(t *domain.WagerTransaction) transactionResponse {
	out := transactionResponse{
		TransactionID:                  t.ID().String(),
		Origin:                         string(t.Origin()),
		Kind:                           string(t.Kind()),
		Status:                         string(t.Status()),
		ProviderID:                     optString(t.ProviderID()),
		ExternalTransactionID:          optString(t.ExternalTransactionID()),
		PlayerID:                       t.PlayerID().String(),
		WalletID:                       t.WalletID().String(),
		RoundID:                        optString(t.RoundID()),
		GameID:                         optString(t.GameID()),
		Money:                          t.Money().ToDTO(),
		ReferenceExternalTransactionID: optString(t.ReferenceExternalTransactionID()),
		FailureCode:                    optString(string(t.FailureCode())),
		Attempts:                       t.Attempts(),
		NextAttemptAt:                  optTime(t.NextAttemptAt()),
		ReferenceExpiresAt:             optTime(t.ReferenceExpiresAt()),
		CreatedAt:                      formatTime(t.CreatedAt()),
		UpdatedAt:                      formatTime(t.UpdatedAt()),
		ProcessedAt:                    optTime(t.ProcessedAt()),
	}
	if id := t.ReferenceTransactionID(); id != uuid.Nil {
		out.ReferenceTransactionID = optString(id.String())
	}
	if rb, ok := t.ResultBalance(); ok {
		d := rb.ToDTO()
		out.Balance = &d
	}
	return out
}

// reconciliationResponse is returned by POST /wallets/{id}/reconciliation.
type reconciliationResponse struct {
	WalletID          string   `json:"walletId"`
	StoredBalance     moneyDTO `json:"storedBalance"`
	CalculatedBalance moneyDTO `json:"calculatedBalance"`
	Difference        moneyDTO `json:"difference"`
	Consistent        bool     `json:"consistent"`
	CheckedEntries    int64    `json:"checkedEntries"`
}

func toReconciliationResponse(r app.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID:          r.WalletID.String(),
		StoredBalance:     r.StoredBalance.ToDTO(),
		CalculatedBalance: r.CalculatedBalance.ToDTO(),
		Difference:        r.Difference.ToDTO(),
		Consistent:        r.Consistent,
		CheckedEntries:    r.CheckedEntries,
	}
}

// errorResponse is the body of every error response.
type errorResponse struct {
	Error         errorBody `json:"error"`
	CorrelationID string    `json:"correlationId,omitempty"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func optString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func optTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := formatTime(t)
	return &s
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}
