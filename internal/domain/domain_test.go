package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func openWallet(t *testing.T, initial string) *Wallet {
	t.Helper()
	o, err := OpenWallet(OpenWalletParams{
		WalletID: newID(), PlayerID: newID(), InitialBalance: brl(t, initial), Now: t0, NewID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return o.Wallet
}

type reqOpt func(*RawExternalRequest)

func rawReq(w *Wallet, kind Kind, amount string, opts ...reqOpt) RawExternalRequest {
	ext := "tx-" + uuid.NewString()
	r := RawExternalRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: ext,
		IdempotencyKey:        "provider-a:" + ext,
		PlayerID:              w.PlayerID().String(),
		WalletID:              w.ID().String(),
		RoundID:               "round-1",
		GameID:                "fortune-chimp",
		Kind:                  string(kind),
		Amount:                amount,
		Currency:              "BRL",
	}
	for _, o := range opts {
		o(&r)
	}
	return r
}

func withRef(ext string) reqOpt {
	return func(r *RawExternalRequest) { r.ReferenceExternalTransactionID = ext }
}

func newTx(t *testing.T, raw RawExternalRequest) *WagerTransaction {
	t.Helper()
	req, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatalf("NewExternalRequest: %v", err)
	}
	tx, err := NewExternalTransaction(newID(), req, "corr-1", t0)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func settle(t *testing.T, tx *WagerTransaction, w *Wallet, ref ReferenceState) Settlement {
	t.Helper()
	s, err := Settle(tx, w, ref, SettleContext{Now: t0, NewID: newID, Policy: DefaultReferencePolicy})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	return s
}

func eventTypes(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type()
	}
	return out
}

// --- wallet ----------------------------------------------------------------

func TestWalletDebitCreditInvariants(t *testing.T) {
	w := openWallet(t, "100.00")
	if w.Version() != 1 {
		t.Fatalf("initial version = %d", w.Version())
	}
	e, err := w.Debit(Movement{EntryID: newID(), TransactionID: newID(), Amount: brl(t, "30.00"), At: t0})
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	if e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "70.00" || e.WalletVersion() != 2 {
		t.Fatalf("entry: %+v", e)
	}
	_, err = w.Debit(Movement{EntryID: newID(), TransactionID: newID(), Amount: brl(t, "70.01"), At: t0})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 {
		t.Fatal("failed debit must not change the wallet")
	}
	if _, err := w.Credit(Movement{EntryID: newID(), TransactionID: newID(), Amount: brl(t, "5.00"), At: t0}); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "75.00" || w.Version() != 3 {
		t.Fatalf("after credit: %s v%d", w.Balance(), w.Version())
	}
}

func TestWalletRejectsInvalidMovements(t *testing.T) {
	w := openWallet(t, "10.00")
	usd, _ := money.Parse("1.00", "USD")
	var rej *RejectionError
	if _, err := w.Credit(Movement{EntryID: newID(), TransactionID: newID(), Amount: usd, At: t0}); !errors.As(err, &rej) || rej.Code != FailureCurrencyMismatch {
		t.Fatalf("currency mismatch: %v", err)
	}
	if _, err := w.Credit(Movement{EntryID: newID(), TransactionID: newID(), Amount: brl(t, "0.00"), At: t0}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero movement: %v", err)
	}
	if _, err := w.Credit(Movement{EntryID: newID(), TransactionID: newID(), Amount: money.Money{}, At: t0}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("uninitialised money: %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("version changed on invalid movement")
	}
}

func TestRehydrateWalletValidates(t *testing.T) {
	neg, _ := money.Parse("-1.00", "BRL")
	_, err := RehydrateWallet(WalletSnapshot{ID: newID(), PlayerID: newID(), Balance: neg, Version: 1, CreatedAt: t0, UpdatedAt: t0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative balance: %v", err)
	}
	_, err = RehydrateWallet(WalletSnapshot{ID: newID(), PlayerID: newID(), Balance: brl(t, "1.00"), Version: 0, CreatedAt: t0, UpdatedAt: t0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("version 0: %v", err)
	}
	w, err := RehydrateWallet(WalletSnapshot{ID: newID(), PlayerID: newID(), Balance: brl(t, "5.00"), Version: 7, CreatedAt: t0, UpdatedAt: t0})
	if err != nil || w.Version() != 7 || w.Balance().Amount() != "5.00" {
		t.Fatalf("rehydrate: %v %v", w, err)
	}
}

func TestLedgerEntryArithmetic(t *testing.T) {
	base := LedgerEntryParams{
		ID: newID(), WalletID: newID(), TransactionID: newID(), Direction: DirectionCredit,
		Amount: brl(t, "10.00"), BalanceBefore: brl(t, "5.00"), BalanceAfter: brl(t, "15.00"),
		WalletVersion: 2, CreatedAt: t0,
	}
	if _, err := NewLedgerEntry(base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.BalanceAfter = brl(t, "14.99")
	if _, err := NewLedgerEntry(bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected arithmetic error, got %v", err)
	}
	debit := base
	debit.Direction = DirectionDebit
	debit.BalanceBefore = brl(t, "15.00")
	debit.BalanceAfter = brl(t, "5.00")
	if _, err := NewLedgerEntry(debit); err != nil {
		t.Fatal(err)
	}
	negative := debit
	negative.BalanceBefore = brl(t, "5.00")
	negative.BalanceAfter, _ = money.Parse("-5.00", "BRL")
	if _, err := NewLedgerEntry(negative); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative balance: %v", err)
	}
}

// --- opening ---------------------------------------------------------------

func TestOpenWalletPositiveBalance(t *testing.T) {
	o, err := OpenWallet(OpenWalletParams{
		WalletID: newID(), PlayerID: newID(), InitialBalance: brl(t, "1000.00"),
		CorrelationID: "corr-open", Now: t0, NewID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Wallet.Version() != 1 || o.Wallet.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet: %s v%d", o.Wallet.Balance(), o.Wallet.Version())
	}
	tx := o.Transaction
	if tx == nil || tx.Kind() != KindOpening || tx.Status() != StatusProcessed || tx.Origin() != OriginInternal {
		t.Fatalf("opening tx: %+v", tx)
	}
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" || tx.RoundID() != "" {
		t.Fatal("OPENING must not carry external metadata")
	}
	if o.Entry == nil || o.Entry.Direction() != DirectionCredit || o.Entry.WalletVersion() != 1 ||
		o.Entry.BalanceBefore().Amount() != "0.00" || o.Entry.BalanceAfter().Amount() != "1000.00" {
		t.Fatalf("entry: %+v", o.Entry)
	}
	got := eventTypes(o.Events)
	if strings.Join(got, ",") != EventWagerTransactionProcessed+","+EventWalletBalanceChanged {
		t.Fatalf("events: %v", got)
	}
	b, _ := json.Marshal(o.Events[0])
	if strings.Contains(string(b), "providerId") || strings.Contains(string(b), "roundId") {
		t.Fatalf("OPENING event must omit external metadata: %s", b)
	}
	var env map[string]any
	_ = json.Unmarshal(b, &env)
	if env["correlationId"] != "corr-open" || env["version"].(float64) != 1 {
		t.Fatalf("envelope: %s", b)
	}
}

func TestOpenWalletZeroBalance(t *testing.T) {
	o, err := OpenWallet(OpenWalletParams{
		WalletID: newID(), PlayerID: newID(), InitialBalance: brl(t, "0.00"), Now: t0, NewID: newID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Transaction != nil || o.Entry != nil || len(o.Events) != 0 {
		t.Fatal("zero opening must not create OPENING, ledger or events")
	}
}

// --- requests / idempotency hash -------------------------------------------

func TestExternalRequestValidation(t *testing.T) {
	w := openWallet(t, "0.00")
	cases := []struct {
		name string
		raw  RawExternalRequest
	}{
		{"opening kind", rawReq(w, KindOpening, "1.00")},
		{"unknown kind", rawReq(w, "JACKPOT", "1.00")},
		{"bet zero", rawReq(w, KindBet, "0.00")},
		{"win zero", rawReq(w, KindWin, "0.00")},
		{"refund zero", rawReq(w, KindRefund, "0.00", withRef("x"))},
		{"rollback zero", rawReq(w, KindRollback, "0.00", withRef("x"))},
		{"loss non zero", rawReq(w, KindLoss, "1.00")},
		{"negative", rawReq(w, KindBet, "-1.00")},
		{"scale", rawReq(w, KindBet, "1.001")},
		{"nan", rawReq(w, KindBet, "NaN")},
		{"sci", rawReq(w, KindBet, "1e2")},
		{"refund without ref", rawReq(w, KindRefund, "1.00")},
		{"rollback without ref", rawReq(w, KindRollback, "1.00")},
		{"bet with ref", rawReq(w, KindBet, "1.00", withRef("x"))},
		{"bad player", rawReq(w, KindBet, "1.00", func(r *RawExternalRequest) { r.PlayerID = "nope" })},
		{"missing provider", rawReq(w, KindBet, "1.00", func(r *RawExternalRequest) { r.ProviderID = "" })},
		{"lower currency", rawReq(w, KindBet, "1.00", func(r *RawExternalRequest) { r.Currency = "brl" })},
		{"self reference", rawReq(w, KindRefund, "1.00", func(r *RawExternalRequest) {
			r.ReferenceExternalTransactionID = r.ExternalTransactionID
		})},
	}
	for _, c := range cases {
		if _, err := NewExternalRequest(c.raw); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: expected ErrInvalidArgument, got %v", c.name, err)
		}
	}
	// zero policy: LOSS accepts exactly zero
	if _, err := NewExternalRequest(rawReq(w, KindLoss, "0.00")); err != nil {
		t.Errorf("LOSS 0.00: %v", err)
	}
	if _, err := NewExternalRequest(rawReq(w, KindLoss, "0")); err != nil {
		t.Errorf("LOSS 0: %v", err)
	}
}

func TestPayloadHashCanonical(t *testing.T) {
	w := openWallet(t, "0.00")
	raw := rawReq(w, KindBet, "25")
	a, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Equivalent forms: normalised amount, upper-case UUID, other idempotency key.
	raw2 := raw
	raw2.Amount = "25.00"
	raw2.PlayerID = strings.ToUpper(raw.PlayerID)
	raw2.IdempotencyKey = "another-key"
	b, err := NewExternalRequest(raw2)
	if err != nil {
		t.Fatal(err)
	}
	if a.PayloadHash() != b.PayloadHash() {
		t.Fatal("equivalent payloads must hash equally (key and formatting excluded)")
	}
	raw3 := raw
	raw3.Amount = "25.01"
	c, _ := NewExternalRequest(raw3)
	if a.PayloadHash() == c.PayloadHash() {
		t.Fatal("different amount must change the hash")
	}
	raw4 := raw
	raw4.RoundID = "round-2"
	d, _ := NewExternalRequest(raw4)
	if a.PayloadHash() == d.PayloadHash() {
		t.Fatal("different round must change the hash")
	}
	tx, _ := NewExternalTransaction(newID(), a, "", t0)
	if !tx.SameRequest(b) || tx.SameRequest(c) {
		t.Fatal("SameRequest must compare payload hashes (conflict detection)")
	}
}

// --- state machine ---------------------------------------------------------

func TestTransitions(t *testing.T) {
	w := openWallet(t, "10.00")
	tx := newTx(t, rawReq(w, KindRefund, "1.00", withRef("bet-1")))
	if tx.Status() != StatusPending {
		t.Fatalf("initial status %s", tx.Status())
	}
	if err := tx.MarkPendingReference(t0.Add(time.Second), t0.Add(time.Minute), t0); err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkPendingReference(t0.Add(2*time.Second), t0.Add(time.Hour), t0); err != nil {
		t.Fatal(err)
	}
	if tx.Attempts() != 2 || !tx.ReferenceExpiresAt().Equal(t0.Add(time.Minute)) {
		t.Fatalf("attempts=%d expires=%v", tx.Attempts(), tx.ReferenceExpiresAt())
	}
	if err := tx.MarkRejected(FailureReferenceNotFound, w.Balance(), t0); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func() error{
		"processed": func() error { return tx.MarkProcessed(w.Balance(), t0) },
		"rejected":  func() error { return tx.MarkRejected(FailureInsufficientFunds, w.Balance(), t0) },
		"failed":    func() error { return tx.MarkFailed(FailureProcessingFailed, t0) },
		"pending":   func() error { return tx.MarkPendingReference(t0.Add(time.Second), t0, t0) },
	} {
		if err := f(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s from terminal: %v", name, err)
		}
	}
	if _, err := Settle(tx, w, ReferenceState{}, SettleContext{Now: t0, NewID: newID}); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("settle of terminal: %v", err)
	}
}

func TestMarkFailedRequiresCode(t *testing.T) {
	w := openWallet(t, "10.00")
	tx := newTx(t, rawReq(w, KindBet, "1.00"))
	if err := tx.MarkFailed("", t0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := FailPermanently(tx, t0); err != nil || tx.Status() != StatusFailed || tx.FailureCode() != FailureProcessingFailed {
		t.Fatalf("FailPermanently: %v %s", err, tx.Status())
	}
}

// --- settlement rules ------------------------------------------------------

func TestBet(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, rawReq(w, KindBet, "80.00"))
	s := settle(t, tx, w, ReferenceState{})
	if tx.Status() != StatusProcessed || w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("bet: %s %s v%d", tx.Status(), w.Balance(), w.Version())
	}
	if s.Entry == nil || s.Entry.Direction() != DirectionDebit {
		t.Fatal("bet must produce a debit entry")
	}
	if got := eventTypes(s.Events); strings.Join(got, ",") != "WagerTransactionProcessed,WalletBalanceChanged" {
		t.Fatalf("events %v", got)
	}
	rb, _ := tx.ResultBalance()
	if rb.Amount() != "20.00" {
		t.Fatalf("result balance %s", rb)
	}

	// Second 80.00 bet: insufficient funds, rejected, no movement.
	tx2 := newTx(t, rawReq(w, KindBet, "80.00"))
	s2 := settle(t, tx2, w, ReferenceState{})
	if tx2.Status() != StatusRejected || tx2.FailureCode() != FailureInsufficientFunds || s2.Entry != nil {
		t.Fatalf("second bet: %s %s", tx2.Status(), tx2.FailureCode())
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatal("rejected bet changed the wallet")
	}
	if got := eventTypes(s2.Events); strings.Join(got, ",") != "WagerTransactionRejected" {
		t.Fatalf("events %v", got)
	}
}

func TestWinAndLoss(t *testing.T) {
	w := openWallet(t, "10.00")
	win := newTx(t, rawReq(w, KindWin, "5.50"))
	s := settle(t, win, w, ReferenceState{})
	if win.Status() != StatusProcessed || w.Balance().Amount() != "15.50" || s.Entry.Direction() != DirectionCredit {
		t.Fatalf("win: %s %s", win.Status(), w.Balance())
	}
	loss := newTx(t, rawReq(w, KindLoss, "0.00"))
	ls := settle(t, loss, w, ReferenceState{})
	if loss.Status() != StatusProcessed || ls.Entry != nil || w.Version() != 2 {
		t.Fatalf("loss: %s entry=%v v%d", loss.Status(), ls.Entry, w.Version())
	}
	if got := eventTypes(ls.Events); strings.Join(got, ",") != "WagerTransactionProcessed" {
		t.Fatalf("LOSS must only emit WagerTransactionProcessed, got %v", got)
	}
}

func TestCurrencyAndPlayerMismatch(t *testing.T) {
	w := openWallet(t, "10.00")
	usd := rawReq(w, KindBet, "1.00", func(r *RawExternalRequest) { r.Currency = "USD" })
	tx := newTx(t, usd)
	settle(t, tx, w, ReferenceState{})
	if tx.FailureCode() != FailureCurrencyMismatch {
		t.Fatalf("got %s", tx.FailureCode())
	}
	lossUSD := newTx(t, rawReq(w, KindLoss, "0.00", func(r *RawExternalRequest) { r.Currency = "USD" }))
	settle(t, lossUSD, w, ReferenceState{})
	if lossUSD.FailureCode() != FailureCurrencyMismatch {
		t.Fatalf("LOSS must require the wallet currency, got %s", lossUSD.FailureCode())
	}
	other := newTx(t, rawReq(w, KindBet, "1.00", func(r *RawExternalRequest) { r.PlayerID = newID().String() }))
	settle(t, other, w, ReferenceState{})
	if other.FailureCode() != FailurePlayerWalletMismatch {
		t.Fatalf("got %s", other.FailureCode())
	}
}

func processedBet(t *testing.T, w *Wallet, amount string) *WagerTransaction {
	t.Helper()
	bet := newTx(t, rawReq(w, KindBet, amount))
	settle(t, bet, w, ReferenceState{})
	if bet.Status() != StatusProcessed {
		t.Fatalf("bet not processed: %s", bet.FailureCode())
	}
	return bet
}

func TestRefund(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := processedBet(t, w, "30.00")
	refund := newTx(t, rawReq(w, KindRefund, "30.00", withRef(bet.ExternalTransactionID())))
	s := settle(t, refund, w, ReferenceState{Transaction: bet})
	if refund.Status() != StatusProcessed || w.Balance().Amount() != "100.00" || s.Entry.Direction() != DirectionCredit {
		t.Fatalf("refund: %s %s", refund.Status(), w.Balance())
	}
	if refund.ReferenceTransactionID() != bet.ID() {
		t.Fatal("reference not resolved")
	}
	// Second reversal of the same bet is refused.
	again := newTx(t, rawReq(w, KindRefund, "30.00", withRef(bet.ExternalTransactionID())))
	settle(t, again, w, ReferenceState{Transaction: bet, AlreadyReversed: true})
	if again.FailureCode() != FailureReferenceAlreadyReversed || w.Balance().Amount() != "100.00" {
		t.Fatalf("double refund: %s %s", again.FailureCode(), w.Balance())
	}
	// ROLLBACK of the same bet (already refunded) is also refused.
	rb := newTx(t, rawReq(w, KindRollback, "30.00", withRef(bet.ExternalTransactionID())))
	settle(t, rb, w, ReferenceState{Transaction: bet, AlreadyReversed: true})
	if rb.FailureCode() != FailureReferenceAlreadyReversed {
		t.Fatalf("rollback after refund: %s", rb.FailureCode())
	}
}

func TestReversalRules(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := processedBet(t, w, "30.00")
	cases := []struct {
		name string
		raw  RawExternalRequest
		ref  ReferenceState
		code FailureCode
	}{
		{"partial", rawReq(w, KindRefund, "10.00", withRef(bet.ExternalTransactionID())), ReferenceState{Transaction: bet}, FailureReversalAmountMismatch},
		{"round mismatch", rawReq(w, KindRefund, "30.00", withRef(bet.ExternalTransactionID()), func(r *RawExternalRequest) { r.RoundID = "other" }), ReferenceState{Transaction: bet}, FailureReferenceMismatch},
		{"provider mismatch", rawReq(w, KindRefund, "30.00", withRef(bet.ExternalTransactionID()), func(r *RawExternalRequest) { r.ProviderID = "provider-b" }), ReferenceState{Transaction: bet}, FailureReferenceMismatch},
	}
	for _, c := range cases {
		tx := newTx(t, c.raw)
		settle(t, tx, w, c.ref)
		if tx.Status() != StatusRejected || tx.FailureCode() != c.code {
			t.Errorf("%s: %s %s", c.name, tx.Status(), tx.FailureCode())
		}
	}
	// REFUND of a WIN is not allowed.
	win := newTx(t, rawReq(w, KindWin, "5.00"))
	settle(t, win, w, ReferenceState{})
	refundWin := newTx(t, rawReq(w, KindRefund, "5.00", withRef(win.ExternalTransactionID())))
	settle(t, refundWin, w, ReferenceState{Transaction: win})
	if refundWin.FailureCode() != FailureReferenceKindNotAllowed {
		t.Fatalf("refund of win: %s", refundWin.FailureCode())
	}
	// Reference that ended REJECTED.
	rejected := newTx(t, rawReq(w, KindBet, "9999.00"))
	settle(t, rejected, w, ReferenceState{})
	rb := newTx(t, rawReq(w, KindRollback, "9999.00", withRef(rejected.ExternalTransactionID())))
	settle(t, rb, w, ReferenceState{Transaction: rejected})
	if rb.FailureCode() != FailureReferenceNotProcessed {
		t.Fatalf("rollback of rejected: %s", rb.FailureCode())
	}
	if w.Balance().Amount() != "75.00" {
		t.Fatalf("balance changed by rejections: %s", w.Balance())
	}
}

func TestRollbackDirections(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := processedBet(t, w, "40.00") // 60
	rbBet := newTx(t, rawReq(w, KindRollback, "40.00", withRef(bet.ExternalTransactionID())))
	s := settle(t, rbBet, w, ReferenceState{Transaction: bet})
	if s.Entry.Direction() != DirectionCredit || w.Balance().Amount() != "100.00" {
		t.Fatalf("rollback bet: %s", w.Balance())
	}
	win := newTx(t, rawReq(w, KindWin, "50.00")) // 150
	settle(t, win, w, ReferenceState{})
	rbWin := newTx(t, rawReq(w, KindRollback, "50.00", withRef(win.ExternalTransactionID())))
	s = settle(t, rbWin, w, ReferenceState{Transaction: win})
	if s.Entry.Direction() != DirectionDebit || w.Balance().Amount() != "100.00" {
		t.Fatalf("rollback win: %s", w.Balance())
	}
	// ROLLBACK of a REFUND debits the refunded amount back.
	bet2 := processedBet(t, w, "10.00") // 90
	refund := newTx(t, rawReq(w, KindRefund, "10.00", withRef(bet2.ExternalTransactionID())))
	settle(t, refund, w, ReferenceState{Transaction: bet2}) // 100
	rbRefund := newTx(t, rawReq(w, KindRollback, "10.00", withRef(refund.ExternalTransactionID())))
	s = settle(t, rbRefund, w, ReferenceState{Transaction: refund})
	if s.Entry.Direction() != DirectionDebit || w.Balance().Amount() != "90.00" {
		t.Fatalf("rollback refund: %s", w.Balance())
	}
	// ROLLBACK of a ROLLBACK is not allowed.
	rbrb := newTx(t, rawReq(w, KindRollback, "10.00", withRef(rbRefund.ExternalTransactionID())))
	settle(t, rbrb, w, ReferenceState{Transaction: rbRefund})
	if rbrb.FailureCode() != FailureReferenceKindNotAllowed {
		t.Fatalf("rollback of rollback: %s", rbrb.FailureCode())
	}
}

func TestRollbackInsufficientFundsHasDistinctCode(t *testing.T) {
	w := openWallet(t, "0.00")
	win := newTx(t, rawReq(w, KindWin, "50.00"))
	settle(t, win, w, ReferenceState{})
	bet := processedBet(t, w, "45.00") // 5 left
	_ = bet
	rb := newTx(t, rawReq(w, KindRollback, "50.00", withRef(win.ExternalTransactionID())))
	s := settle(t, rb, w, ReferenceState{Transaction: win})
	if rb.Status() != StatusRejected || rb.FailureCode() != FailureReversalInsufficientFunds || s.Entry != nil {
		t.Fatalf("got %s %s", rb.Status(), rb.FailureCode())
	}
	if rb.FailureCode() == FailureInsufficientFunds {
		t.Fatal("reversal must not reuse the BET code")
	}
	if w.Balance().Amount() != "5.00" {
		t.Fatalf("balance %s", w.Balance())
	}
}

func TestWinWithBetReference(t *testing.T) {
	w := openWallet(t, "10.00")
	bet := processedBet(t, w, "5.00")
	win := newTx(t, rawReq(w, KindWin, "20.00", withRef(bet.ExternalTransactionID())))
	settle(t, win, w, ReferenceState{Transaction: bet})
	if win.Status() != StatusProcessed || win.ReferenceTransactionID() != bet.ID() {
		t.Fatalf("win with ref: %s", win.FailureCode())
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	w := openWallet(t, "100.00")
	policy := ReferencePolicy{MaxAttempts: 3, TTL: time.Hour, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}
	refund := newTx(t, rawReq(w, KindRefund, "30.00", withRef("bet-late")))
	ctx := SettleContext{Now: t0, NewID: newID, Policy: policy}

	s, err := Settle(refund, w, ReferenceState{}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refund.Status() != StatusPendingReference || refund.Attempts() != 1 {
		t.Fatalf("status %s attempts %d", refund.Status(), refund.Attempts())
	}
	if got := eventTypes(s.Events); strings.Join(got, ",") != EventWagerTransactionPendingReference {
		t.Fatalf("events %v", got)
	}
	if !refund.NextAttemptAt().Equal(t0.Add(time.Second)) {
		t.Fatalf("next attempt %v", refund.NextAttemptAt())
	}
	// Retry: still missing -> rescheduled with doubled backoff, no new event.
	ctx.Now = t0.Add(time.Second)
	s, _ = Settle(refund, w, ReferenceState{}, ctx)
	if refund.Status() != StatusPendingReference || len(s.Events) != 0 || !refund.NextAttemptAt().Equal(ctx.Now.Add(2*time.Second)) {
		t.Fatalf("reschedule: %s events=%d next=%v", refund.Status(), len(s.Events), refund.NextAttemptAt())
	}
	// Third attempt exhausts the budget -> REJECTED REFERENCE_NOT_FOUND.
	ctx.Now = t0.Add(3 * time.Second)
	s, _ = Settle(refund, w, ReferenceState{}, ctx)
	if refund.Status() != StatusRejected || refund.FailureCode() != FailureReferenceNotFound {
		t.Fatalf("expire: %s %s", refund.Status(), refund.FailureCode())
	}
	if got := eventTypes(s.Events); strings.Join(got, ",") != EventWagerTransactionRejected {
		t.Fatalf("events %v", got)
	}
	if w.Balance().Amount() != "100.00" {
		t.Fatal("balance changed")
	}
}

func TestPendingReferenceResolvedLater(t *testing.T) {
	w := openWallet(t, "100.00")
	ctx := SettleContext{Now: t0, NewID: newID, Policy: DefaultReferencePolicy}
	refund := newTx(t, rawReq(w, KindRefund, "30.00", withRef("bet-late"),
		func(r *RawExternalRequest) { r.ExternalTransactionID = "refund-1"; r.IdempotencyKey = "k" }))
	if _, err := Settle(refund, w, ReferenceState{}, ctx); err != nil {
		t.Fatal(err)
	}
	bet := newTx(t, rawReq(w, KindBet, "30.00", func(r *RawExternalRequest) { r.ExternalTransactionID = "bet-late" }))
	settle(t, bet, w, ReferenceState{})
	ctx.Now = t0.Add(time.Second)
	s, err := Settle(refund, w, ReferenceState{Transaction: bet}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refund.Status() != StatusProcessed || w.Balance().Amount() != "100.00" || s.Entry == nil {
		t.Fatalf("late resolution: %s %s", refund.Status(), w.Balance())
	}
}

func TestReferencePendingIsAwaited(t *testing.T) {
	w := openWallet(t, "100.00")
	pendingRef := newTx(t, rawReq(w, KindRefund, "1.00", withRef("missing")))
	rb := newTx(t, rawReq(w, KindRollback, "1.00", withRef(pendingRef.ExternalTransactionID())))
	settle(t, rb, w, ReferenceState{Transaction: pendingRef})
	if rb.Status() != StatusPendingReference {
		t.Fatalf("reference still pending must be awaited, got %s", rb.Status())
	}
}

func TestReferencePolicyDelay(t *testing.T) {
	p := ReferencePolicy{BaseBackoff: time.Second, MaxBackoff: 5 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Errorf("Delay(%d)=%v want %v", i+1, got, w)
		}
	}
}

func TestBalanceChangedEventPayload(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, rawReq(w, KindBet, "25.00"))
	s := settle(t, tx, w, ReferenceState{})
	b, err := json.Marshal(s.Events[1])
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		EventType   string `json:"eventType"`
		AggregateID string `json:"aggregateId"`
		CausationID string `json:"causationId"`
		OccurredAt  string `json:"occurredAt"`
		Data        struct {
			WalletID      string    `json:"walletId"`
			TransactionID string    `json:"transactionId"`
			Direction     string    `json:"direction"`
			Money         money.DTO `json:"money"`
			BalanceBefore money.DTO `json:"balanceBefore"`
			BalanceAfter  money.DTO `json:"balanceAfter"`
			WalletVersion int64     `json:"walletVersion"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	if env.EventType != EventWalletBalanceChanged || env.AggregateID != w.ID().String() ||
		env.CausationID != tx.ID().String() || env.OccurredAt != "2026-09-08T12:00:00.000Z" {
		t.Fatalf("envelope: %s", b)
	}
	d := env.Data
	if d.Direction != "DEBIT" || d.Money.Amount != "25.00" || d.BalanceBefore.Amount != "100.00" ||
		d.BalanceAfter.Amount != "75.00" || d.WalletVersion != 2 || d.TransactionID != tx.ID().String() {
		t.Fatalf("data: %s", b)
	}
}
