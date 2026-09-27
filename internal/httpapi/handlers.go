// Package httpapi exposes the REST API with net/http (Go 1.22 routing
// patterns). Handlers only translate HTTP <-> use cases; all business rules
// live in the domain and app packages.
//
// HTTP status contract (see README for bodies):
//
//	200 PROCESSED (new or replay)          202 PENDING / PENDING_REFERENCE
//	201 wallet created                     400 invalid input / missing key
//	401 missing, invalid or expired token  403 authenticated but not allowed
//	404 unknown (or not visible) resource  409 idempotency / uniqueness conflict
//	422 business rejection (REJECTED)      503 transient unavailability (Retry-After)
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/auth"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// maxBodyBytes bounds request bodies.
const maxBodyBytes = 64 << 10

// Stable API error codes.
const (
	codeInvalidJSON           = "INVALID_JSON"
	codeValidation            = "VALIDATION_ERROR"
	codeMissingIdempotencyKey = "MISSING_IDEMPOTENCY_KEY"
	codeUnauthenticated       = "UNAUTHENTICATED"
	codeForbidden             = "FORBIDDEN"
	codeNotFound              = "NOT_FOUND"
	codeWalletNotFound        = "WALLET_NOT_FOUND"
	codeWalletExists          = "WALLET_ALREADY_EXISTS"
	codeIdempotencyConflict   = "IDEMPOTENCY_KEY_CONFLICT"
	codeExternalIDConflict    = "EXTERNAL_TRANSACTION_CONFLICT"
	codeUnavailable           = "SERVICE_UNAVAILABLE"
	codeInternal              = "INTERNAL_ERROR"
	codeInvalidCursor         = "INVALID_CURSOR"
)

// Handlers holds the use cases used by the HTTP layer.
type Handlers struct {
	wagering *app.WageringService
	wallets  *app.WalletService
	log      *slog.Logger
}

// NewHandlers builds the handlers.
func NewHandlers(wagering *app.WageringService, wallets *app.WalletService, log *slog.Logger) *Handlers {
	return &Handlers{wagering: wagering, wallets: wallets, log: log}
}

// --- wallets (internal service only) -------------------------------------------

func (h *Handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil || playerID == uuid.Nil || len(req.PlayerID) != 36 {
		writeError(w, r, http.StatusBadRequest, codeValidation, "playerId must be a UUID")
		return
	}
	initial, err := money.ParseNonNegative(req.InitialBalance.Amount, req.InitialBalance.Currency)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeValidation, "initialBalance: "+err.Error())
		return
	}
	wallet, err := h.wallets.OpenWallet(r.Context(), playerID, initial, correlationID(r.Context()))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletResponse(wallet))
}

func (h *Handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	wallet, err := h.wallets.GetWallet(r.Context(), id)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wallet))
}

func (h *Handlers) getLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := app.DefaultLedgerLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > app.MaxLedgerLimit {
			writeError(w, r, http.StatusBadRequest, codeValidation, "limit must be within 1..200")
			return
		}
		limit = n
	}
	page, err := h.wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toLedgerResponse(id.String(), page))
}

func (h *Handlers) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	rec, err := h.wallets.Reconcile(r.Context(), id)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toReconciliationResponse(rec))
}

// --- wagering ------------------------------------------------------------------

func (h *Handlers) submitTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, r, http.StatusBadRequest, codeMissingIdempotencyKey, "the Idempotency-Key header is required")
		return
	}
	var body submitRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	// The authenticated identity determines the provider: a provider can
	// only submit (and replay) operations on its own behalf.
	if body.ProviderID != p.ProviderID {
		writeError(w, r, http.StatusForbidden, codeForbidden, "providerId does not match the authenticated provider")
		return
	}
	req, err := domain.NewExternalRequest(body.toRaw(key))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeValidation, err.Error())
		return
	}
	res, err := h.wagering.Submit(r.Context(), app.SubmitCommand{
		Request:       req,
		CorrelationID: correlationID(r.Context()),
		Source:        "http",
	})
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, statusFor(res.Transaction.Status()), toSubmitResponse(res.Transaction, res.Replay))
}

// statusFor maps a transaction status to the HTTP status of the submit.
func statusFor(s domain.Status) int {
	switch s {
	case domain.StatusProcessed:
		return http.StatusOK
	case domain.StatusRejected:
		return http.StatusUnprocessableEntity
	case domain.StatusFailed:
		return http.StatusInternalServerError
	default: // PENDING, PENDING_REFERENCE
		return http.StatusAccepted
	}
}

func (h *Handlers) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	t, err := h.wagering.GetTransaction(r.Context(), id)
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	p, _ := auth.FromContext(r.Context())
	// Internal OPENING transactions have no provider: only the wallet
	// operator sees them. Another provider's transaction is reported as
	// 404 so its existence is not disclosed.
	if !p.IsWalletOperator() && (t.ProviderID() == "" || !p.CanAccessProvider(t.ProviderID())) {
		writeError(w, r, http.StatusNotFound, codeNotFound, "transaction not found")
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

func (h *Handlers) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	p, _ := auth.FromContext(r.Context())
	if !p.CanAccessProvider(providerID) {
		writeError(w, r, http.StatusForbidden, codeForbidden, "access to another provider's transactions is not allowed")
		return
	}
	t, err := h.wagering.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		h.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

// --- helpers -------------------------------------------------------------------

// writeAppError maps application/domain errors to HTTP responses.
func (h *Handlers) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, r, http.StatusNotFound, codeWalletNotFound, "wallet not found")
	case errors.Is(err, app.ErrNotFound):
		writeError(w, r, http.StatusNotFound, codeNotFound, "not found")
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeError(w, r, http.StatusConflict, codeWalletExists, err.Error())
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, r, http.StatusConflict, codeIdempotencyConflict, err.Error())
	case errors.Is(err, app.ErrExternalIDConflict):
		writeError(w, r, http.StatusConflict, codeExternalIDConflict, err.Error())
	case errors.Is(err, app.ErrInvalidCursor):
		writeError(w, r, http.StatusBadRequest, codeInvalidCursor, "invalid cursor")
	case errors.Is(err, domain.ErrInvalidArgument):
		writeError(w, r, http.StatusBadRequest, codeValidation, err.Error())
	case app.IsTransient(err) || errors.Is(err, context.DeadlineExceeded):
		h.log.WarnContext(r.Context(), "transient failure", "error", err)
		w.Header().Set("Retry-After", "1")
		writeError(w, r, http.StatusServiceUnavailable, codeUnavailable, "temporarily unavailable, retry with the same Idempotency-Key")
	default:
		h.log.ErrorContext(r.Context(), "unexpected error", "error", err)
		writeError(w, r, http.StatusInternalServerError, codeInternal, "internal error")
	}
}

// decodeJSON strictly decodes the body (unknown fields rejected, single
// JSON value, bounded size).
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidJSON, "body too large or unreadable")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, r, http.StatusBadRequest, codeInvalidJSON, "invalid JSON body: "+err.Error())
		return false
	}
	if dec.More() {
		writeError(w, r, http.StatusBadRequest, codeInvalidJSON, "body must contain a single JSON object")
		return false
	}
	return true
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, codeValidation, name+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	writeJSON(w, status, errorResponse{
		Error:         errorBody{Code: code, Message: msg},
		CorrelationID: correlationID(r.Context()),
	})
}
