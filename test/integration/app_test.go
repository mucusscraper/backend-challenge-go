//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/mucusscraper/backend-challenge-go/internal/bootstrap"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/httpapi"
	tu "github.com/mucusscraper/backend-challenge-go/test/testutil"
)

// running é uma instância em processo da aplicação fx completa.
type running struct {
	app     *fxtest.App
	base    string
	server  *httpapi.Server
	workers *bootstrap.Workers
	pool    *pgxpool.Pool
}

func testConfig(t *testing.T, q tu.TestQueues) config.Config {
	env := map[string]string{
		"INSTANCE_ID":            "it-" + uuid.NewString()[:8],
		"LOG_LEVEL":              "error",
		"HTTP_ADDR":              "127.0.0.1:0",
		"DATABASE_URL":           tu.AppDSN,
		"AWS_REGION":             "us-east-1",
		"AWS_ENDPOINT_URL":       tu.SQSEndpoint,
		"AWS_ACCESS_KEY_ID":      "test",
		"AWS_SECRET_ACCESS_KEY":  "test",
		"SQS_INBOUND_QUEUE":      q.Config.InboundQueue,
		"SQS_INBOUND_DLQ":        q.Config.InboundDLQ,
		"SQS_EVENTS_QUEUE":       q.Config.EventsQueue,
		"SQS_CONSUMER_NAME":      q.Config.ConsumerName,
		"SQS_WAIT_TIME":          "1s",
		"SQS_VISIBILITY_TIMEOUT": "5s",
		"SQS_HANDLER_TIMEOUT":    "4s",
		"OIDC_ISSUER":            tu.OIDCIssuer,
		"OIDC_AUDIENCE":          "wagering-api",
		"PENDING_POLL_INTERVAL":  "100ms",
		"PENDING_BASE_BACKOFF":   "100ms",
		"PENDING_MAX_BACKOFF":    "200ms",
		"OUTBOX_POLL_INTERVAL":   "100ms",
		"SHUTDOWN_TIMEOUT":       "10s",
	}
	cfg, err := config.LoadFrom(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func startApp(t *testing.T, cfg config.Config) *running {
	t.Helper()
	tu.Migrate(t)
	r := &running{}
	r.app = fxtest.New(t, bootstrap.Options(cfg, fx.Populate(&r.server, &r.workers, &r.pool)))
	r.app.RequireStart()
	r.base = "http://" + r.server.Addr()
	return r
}

type apiResponse struct {
	status int
	body   map[string]any
}

func call(t *testing.T, method, url, token string, body any, headers ...string) apiResponse {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := apiResponse{status: resp.StatusCode, body: map[string]any{}}
	_ = json.NewDecoder(resp.Body).Decode(&out.body)
	return out
}

func wagerBody(provider string, playerID, walletID any, kind, amount, ext, ref string) map[string]any {
	b := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": playerID, "walletId": walletID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		b["referenceExternalTransactionId"] = ref
	}
	return b
}

// TestFxLifecycle inicia e para a composição completa e verifica que os
// workers terminaram e os recursos foram liberados.
func TestFxLifecycle(t *testing.T) {
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	r := startApp(t, testConfig(t, q))

	if res := call(t, "GET", r.base+"/health/live", "", nil); res.status != 200 {
		t.Fatalf("live %d", res.status)
	}
	if res := call(t, "GET", r.base+"/health/ready", "", nil); res.status != 200 {
		t.Fatalf("ready %d %v", res.status, res.body)
	}
	resp, err := http.Get(r.base + "/metrics")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("metrics %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "wagering_outbox_lag_seconds") {
		t.Fatal("metrics missing")
	}

	r.app.RequireStop()
	for name, done := range map[string]<-chan struct{}{"outbox": r.workers.Outbox.Done(), "pending": r.workers.Pending.Done()} {
		select {
		case <-done:
		default:
			t.Fatalf("%s worker still running after stop", name)
		}
	}
	if err := r.pool.Ping(context.Background()); err == nil {
		t.Fatal("pool still usable after stop")
	}
	if _, err := http.Get(r.base + "/health/live"); err == nil {
		t.Fatal("server still accepting connections after stop")
	}
}

// TestAuthentication: tokens ausentes, inválidos, adulterados e expirados são
// rejeitados com 401 e sem efeito financeiro.
func TestAuthentication(t *testing.T) {
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	r := startApp(t, testConfig(t, q))
	defer r.app.RequireStop()

	op := tu.Token(t, tu.WalletService, tu.WalletServiceSec)
	w := call(t, "POST", r.base+"/wallets", op, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}})
	if w.status != 201 {
		t.Fatalf("open wallet %d %v", w.status, w.body)
	}
	body := wagerBody("provider-a", w.body["playerId"], w.body["id"], "BET", "10.00", "auth-"+uuid.NewString(), "")

	valid := tu.Token(t, tu.ProviderA, tu.ProviderASecret)
	parts := strings.Split(valid, ".")
	tampered := parts[0] + "." + parts[1] + "." + strings.Repeat("A", len(parts[2]))
	short := tu.Token(t, tu.ShortLived, tu.ShortLivedSecret)
	time.Sleep(3500 * time.Millisecond) // lifespan is 2s

	for name, tok := range map[string]string{"missing": "", "garbage": "not-a-jwt", "tampered": tampered, "expired": short} {
		res := call(t, "POST", r.base+"/wagering/transactions", tok, body, "Idempotency-Key", "k-"+uuid.NewString())
		if res.status != 401 {
			t.Errorf("%s token: status %d", name, res.status)
		}
	}
	// A carteira não foi alterada.
	got := call(t, "GET", r.base+"/wallets/"+w.body["id"].(string), op, nil)
	if got.body["balance"].(map[string]any)["amount"] != "100.00" || got.body["version"].(float64) != 1 {
		t.Fatalf("wallet changed: %v", got.body)
	}
}

// TestAuthorizationAndProviderIsolation cobre roles e escopo por provedor,
// incluindo replays e consultas.
func TestAuthorizationAndProviderIsolation(t *testing.T) {
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	r := startApp(t, testConfig(t, q))
	defer r.app.RequireStop()

	op := tu.Token(t, tu.WalletService, tu.WalletServiceSec)
	pa := tu.Token(t, tu.ProviderA, tu.ProviderASecret)
	pb := tu.Token(t, tu.ProviderB, tu.ProviderBSecret)
	noRole := tu.Token(t, tu.NoRoleClient, tu.NoRoleClientSecret)

	// Operações de carteira são exclusivas do serviço interno.
	openBody := map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}
	for name, tok := range map[string]string{"provider": pa, "no-role": noRole} {
		if res := call(t, "POST", r.base+"/wallets", tok, openBody); res.status != 403 {
			t.Errorf("%s opening wallet: %d", name, res.status)
		}
	}
	w := call(t, "POST", r.base+"/wallets", op, openBody)
	walletID := w.body["id"].(string)
	if res := call(t, "GET", r.base+"/wallets/"+walletID, pa, nil); res.status != 403 {
		t.Errorf("provider reading wallet: %d", res.status)
	}
	if res := call(t, "POST", r.base+"/wallets/"+walletID+"/reconciliation", pa, nil); res.status != 403 {
		t.Errorf("provider reconciling: %d", res.status)
	}
	// O serviço interno não pode submeter operações de provedor.
	ext := "iso-" + uuid.NewString()
	key := "provider-a:" + ext
	body := wagerBody("provider-a", w.body["playerId"], walletID, "BET", "10.00", ext, "")
	if res := call(t, "POST", r.base+"/wagering/transactions", op, body, "Idempotency-Key", key); res.status != 403 {
		t.Errorf("operator submitting: %d", res.status)
	}

	// Provedor A submete sua aposta.
	created := call(t, "POST", r.base+"/wagering/transactions", pa, body, "Idempotency-Key", key)
	if created.status != 200 || created.body["status"] != "PROCESSED" || created.body["idempotentReplay"] != false {
		t.Fatalf("provider A submit: %d %v", created.status, created.body)
	}
	txID := created.body["transactionId"].(string)

	// Provedor B não pode submeter (nem replay) em nome do provedor A.
	if res := call(t, "POST", r.base+"/wagering/transactions", pb, body, "Idempotency-Key", key); res.status != 403 {
		t.Errorf("provider B replaying A: %d", res.status)
	}
	// Provedor B não pode ler a transação de A por id nem por id externo.
	if res := call(t, "GET", r.base+"/wagering/transactions/"+txID, pb, nil); res.status != 404 {
		t.Errorf("provider B reading A by id: %d", res.status)
	}
	if res := call(t, "GET", r.base+"/providers/provider-a/wagering/transactions/"+ext, pb, nil); res.status != 403 {
		t.Errorf("provider B reading A by external id: %d", res.status)
	}
	// Provedor B usando seu próprio namespace também não vê a operação de A.
	if res := call(t, "GET", r.base+"/providers/provider-b/wagering/transactions/"+ext, pb, nil); res.status != 404 {
		t.Errorf("provider B own namespace: %d", res.status)
	}
	// Provedor A e o operador podem ler.
	if res := call(t, "GET", r.base+"/wagering/transactions/"+txID, pa, nil); res.status != 200 {
		t.Errorf("provider A reading own: %d", res.status)
	}
	if res := call(t, "GET", r.base+"/providers/provider-a/wagering/transactions/"+ext, op, nil); res.status != 200 {
		t.Errorf("operator reading: %d", res.status)
	}
	// Provedor A faz replay: resultado original, sem novo efeito.
	replay := call(t, "POST", r.base+"/wagering/transactions", pa, body, "Idempotency-Key", key)
	if replay.status != 200 || replay.body["idempotentReplay"] != true || replay.body["transactionId"] != txID {
		t.Fatalf("replay: %d %v", replay.status, replay.body)
	}
	// Reutilização conflitante da chave.
	body2 := wagerBody("provider-a", w.body["playerId"], walletID, "BET", "11.00", ext, "")
	if res := call(t, "POST", r.base+"/wagering/transactions", pa, body2, "Idempotency-Key", key); res.status != 409 {
		t.Errorf("key conflict: %d", res.status)
	}
	// Idempotency-Key ausente e valor inválido resultam em 400.
	if res := call(t, "POST", r.base+"/wagering/transactions", pa, body); res.status != 400 {
		t.Errorf("missing key: %d", res.status)
	}
	bad := wagerBody("provider-a", w.body["playerId"], walletID, "BET", "1e3", "bad-"+uuid.NewString(), "")
	if res := call(t, "POST", r.base+"/wagering/transactions", pa, bad, "Idempotency-Key", "bad"); res.status != 400 {
		t.Errorf("scientific notation: %d", res.status)
	}
	opening := wagerBody("provider-a", w.body["playerId"], walletID, "OPENING", "10.00", "op-"+uuid.NewString(), "")
	if res := call(t, "POST", r.base+"/wagering/transactions", pa, opening, "Idempotency-Key", "op"); res.status != 400 {
		t.Errorf("OPENING via HTTP: %d", res.status)
	}
	// Rejeição de negócio retorna 422 com código de falha.
	big := wagerBody("provider-a", w.body["playerId"], walletID, "BET", "1000.00", "big-"+uuid.NewString(), "")
	res := call(t, "POST", r.base+"/wagering/transactions", pa, big, "Idempotency-Key", "big-"+uuid.NewString())
	if res.status != 422 || res.body["failureCode"] != "INSUFFICIENT_FUNDS" {
		t.Errorf("rejection: %d %v", res.status, res.body)
	}
	// Apenas o único débito autorizado ocorreu.
	wal := call(t, "GET", r.base+"/wallets/"+walletID, op, nil)
	if wal.body["balance"].(map[string]any)["amount"] != "90.00" {
		t.Fatalf("balance %v", wal.body)
	}
	rec := call(t, "POST", r.base+"/wallets/"+walletID+"/reconciliation", op, nil)
	if rec.status != 200 || rec.body["consistent"] != true || rec.body["checkedEntries"].(float64) != 2 {
		t.Fatalf("reconciliation %v", rec.body)
	}
	ledger := call(t, "GET", r.base+"/wallets/"+walletID+"/ledger?limit=1", op, nil)
	if ledger.status != 200 || len(ledger.body["entries"].([]any)) != 1 || ledger.body["nextCursor"] == nil {
		t.Fatalf("ledger page 1 %v", ledger.body)
	}
	page2 := call(t, "GET", fmt.Sprintf("%s/wallets/%s/ledger?limit=1&cursor=%s", r.base, walletID, ledger.body["nextCursor"]), op, nil)
	if page2.status != 200 || len(page2.body["entries"].([]any)) != 1 || page2.body["nextCursor"] != nil {
		t.Fatalf("ledger page 2 %v", page2.body)
	}
}

// TestRestartPreservesPendingAndIdempotency: um REFUND aguarda seu BET na
// instância A; A para; o BET chega; uma nova instância B retoma o
// reembolso. Replays continuam retornando o resultado persistido após reinicializações.
func TestRestartPreservesPendingAndIdempotency(t *testing.T) {
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	cfgA := testConfig(t, q)
	cfgA.Pending.Enabled = false
	a := startApp(t, cfgA)

	op := tu.Token(t, tu.WalletService, tu.WalletServiceSec)
	pa := tu.Token(t, tu.ProviderA, tu.ProviderASecret)
	w := call(t, "POST", a.base+"/wallets", op, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}})
	walletID, playerID := w.body["id"].(string), w.body["playerId"].(string)
	betExt, refundExt := "rs-bet-"+uuid.NewString(), "rs-refund-"+uuid.NewString()
	refundBody := wagerBody("provider-a", playerID, walletID, "REFUND", "40.00", refundExt, betExt)
	pending := call(t, "POST", a.base+"/wagering/transactions", pa, refundBody, "Idempotency-Key", "provider-a:"+refundExt)
	if pending.status != 202 || pending.body["status"] != "PENDING_REFERENCE" {
		t.Fatalf("refund: %d %v", pending.status, pending.body)
	}
	a.app.RequireStop()

	// A aposta chega enquanto nenhuma instância resolve referências.
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	wid, _ := uuid.Parse(walletID)
	wallet, _ := s.Wallets.GetWallet(context.Background(), wid)
	tu.Submit(t, s, tu.Req(t, "provider-a", wallet, domain.KindBet, "40.00", betExt, ""))

	b := startApp(t, testConfig(t, q))
	defer b.app.RequireStop()
	txID := pending.body["transactionId"].(string)
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := call(t, "GET", b.base+"/wagering/transactions/"+txID, pa, nil)
		if got.body["status"] == "PROCESSED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refund not resumed: %v", got.body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	replay := call(t, "POST", b.base+"/wagering/transactions", pa, refundBody, "Idempotency-Key", "provider-a:"+refundExt)
	if replay.status != 200 || replay.body["idempotentReplay"] != true || replay.body["balance"].(map[string]any)["amount"] != "100.00" {
		t.Fatalf("replay after restart: %d %v", replay.status, replay.body)
	}
	tu.AssertConsistent(t, s.Pool, wid)
}
