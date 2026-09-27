//go:build integration || e2e

// Package testutil contém helpers compartilhados pelas suítes de testes de
// integração e e2e. Eles falam com INFRAESTRUTURA REAL (PostgreSQL, Keycloak,
// LocalStack) iniciada com docker compose; nada é mockado.
package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
	"github.com/mucusscraper/backend-challenge-go/internal/messaging"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
	"github.com/mucusscraper/backend-challenge-go/internal/postgres"
)

// Env retorna uma variável de ambiente ou um padrão compatível com docker compose.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Configurações de conexão (substituíveis por variáveis de ambiente).
var (
	OwnerDSN     = Env("TEST_MIGRATION_DATABASE_URL", "postgres://wagering_owner:wagering_owner@localhost:55432/wagering?sslmode=disable")
	AppDSN       = Env("TEST_DATABASE_URL", "postgres://wallet_app:wallet_app@localhost:55432/wagering?sslmode=disable")
	SQSEndpoint  = Env("TEST_AWS_ENDPOINT_URL", "http://localhost:4566")
	KeycloakURL  = Env("TEST_KEYCLOAK_URL", "http://localhost:8080")
	OIDCIssuer   = Env("TEST_OIDC_ISSUER", "http://localhost:8080/realms/wagering")
	TokenURL     = KeycloakURL + "/realms/wagering/protocol/openid-connect/token"
	migrateOnce  sync.Once
	migrateError error
)

// Migrate aplica as migrações uma vez por binário de teste.
func Migrate(t testing.TB) {
	t.Helper()
	migrateOnce.Do(func() {
		m, err := postgres.NewMigrator(OwnerDSN)
		if err != nil {
			migrateError = err
			return
		}
		defer m.Close()
		migrateError = m.Up(context.Background())
	})
	if migrateError != nil {
		t.Fatalf("migrations: %v (is `docker compose up -d postgres` running?)", migrateError)
	}
}

// Logger retorna um logger silencioso para testes (defina TEST_LOG=1 para saída).
func Logger() *slog.Logger {
	if os.Getenv("TEST_LOG") != "" {
		return observability.NewLogger("debug", "test")
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Pool abre um pool pgx com o role restrito de runtime (como em produção).
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	Migrate(t)
	pool, err := postgres.NewPool(config.PostgresConfig{DSN: AppDSN, MaxConns: 40, LockTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.WaitReady(context.Background(), pool, 20*time.Second, Logger()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// OwnerPool abre um pool com o role owner (para testes de adulteração).
func OwnerPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	Migrate(t)
	pool, err := pgxpool.New(context.Background(), OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Services agrupa os casos de uso conectados a um banco de dados real.
type Services struct {
	Pool     *pgxpool.Pool
	UoW      *postgres.UnitOfWork
	Wagering *app.WageringService
	Wallets  *app.WalletService
	Metrics  *observability.Metrics
}

// NewServices conecta os casos de uso como em produção. Cada chamada tem seu
// próprio pool, simulando instâncias independentes no mesmo processo.
func NewServices(t testing.TB, policy domain.ReferencePolicy) *Services {
	t.Helper()
	pool := Pool(t)
	uow := postgres.NewUnitOfWork(pool)
	m := observability.NewMetrics()
	log := Logger()
	return &Services{
		Pool:     pool,
		UoW:      uow,
		Wagering: app.NewWageringService(uow, app.SystemClock, app.NewUUIDv7, policy, log, m),
		Wallets:  app.NewWalletService(uow, app.SystemClock, app.NewUUIDv7, log, m),
		Metrics:  m,
	}
}

// BRL analisa um valor em BRL.
func BRL(t testing.TB, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// OpenWallet abre uma carteira para um novo jogador.
func OpenWallet(t testing.TB, s *Services, initial string) *domain.Wallet {
	t.Helper()
	w, err := s.Wallets.OpenWallet(context.Background(), uuid.Must(uuid.NewV7()), BRL(t, initial), "test")
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	return w
}

// Req constrói uma requisição externa validada.
func Req(t testing.TB, provider string, w *domain.Wallet, kind domain.Kind, amount, extID string, ref string) domain.ExternalRequest {
	t.Helper()
	r, err := domain.NewExternalRequest(RawReq(provider, w, kind, amount, extID, ref))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return r
}

// RawReq constrói uma requisição bruta com chave "{provider}:{extID}".
func RawReq(provider string, w *domain.Wallet, kind domain.Kind, amount, extID, ref string) domain.RawExternalRequest {
	return domain.RawExternalRequest{
		ProviderID:                     provider,
		ExternalTransactionID:          extID,
		IdempotencyKey:                 provider + ":" + extID,
		PlayerID:                       w.PlayerID().String(),
		WalletID:                       w.ID().String(),
		RoundID:                        "round-1",
		GameID:                         "fortune-chimp",
		Kind:                           string(kind),
		Amount:                         amount,
		Currency:                       "BRL",
		ReferenceExternalTransactionID: ref,
	}
}

// Submit é um atalho para uma submissão similar a HTTP.
func Submit(t testing.TB, s *Services, req domain.ExternalRequest) app.SubmitResult {
	t.Helper()
	res, err := s.Wagering.Submit(context.Background(), app.SubmitCommand{Request: req, CorrelationID: "test", Source: "test"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return res
}

// LedgerStats retorna (entradas, créditos-débitos em unidades menores, contagem de débitos)
// calculados diretamente em SQL, independentemente do código da aplicação.
func LedgerStats(t testing.TB, pool *pgxpool.Pool, walletID uuid.UUID) (entries int64, net int64, debits int64) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*),
		       COALESCE(SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE -amount_minor END), 0),
		       COUNT(*) FILTER (WHERE direction='DEBIT')
		  FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&entries, &net, &debits)
	if err != nil {
		t.Fatal(err)
	}
	return
}

// StoredBalance lê o saldo da carteira (unidades menores) e a versão.
func StoredBalance(t testing.TB, pool *pgxpool.Pool, walletID uuid.UUID) (balance, version int64) {
	t.Helper()
	if err := pool.QueryRow(context.Background(),
		`SELECT balance_minor, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version); err != nil {
		t.Fatal(err)
	}
	return
}

// AssertConsistent verifica se saldo armazenado == soma(créditos) - soma(débitos).
func AssertConsistent(t testing.TB, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	bal, _ := StoredBalance(t, pool, walletID)
	_, net, _ := LedgerStats(t, pool, walletID)
	if bal != net {
		t.Fatalf("wallet %s inconsistent: stored=%d ledger=%d", walletID, bal, net)
	}
}

// --- SQS -------------------------------------------------------------------------

// SQSClient retorna um cliente para o LocalStack.
func SQSClient(t testing.TB) *sqs.Client {
	t.Helper()
	c, err := messaging.NewClient(config.AWSConfig{
		Region: "us-east-1", Endpoint: SQSEndpoint, AccessKeyID: "test", SecretAccessKey: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestQueues são filas isoladas criadas para um teste.
type TestQueues struct {
	Config config.SQSConfig
	Queues *messaging.Queues
}

// CreateQueues provisiona uma fila FIFO de entrada isolada com sua DLQ
// (redrive após maxReceive entregas) e uma fila FIFO de eventos, para que os
// testes não interfiram nas instâncias iniciadas pelo docker compose.
func CreateQueues(t testing.TB, c *sqs.Client, maxReceive int, visibility time.Duration) TestQueues {
	t.Helper()
	ctx := context.Background()
	suffix := strings.ReplaceAll(uuid.NewString()[:13], "-", "")
	in, dlq, ev := "it-in-"+suffix+".fifo", "it-dlq-"+suffix+".fifo", "it-ev-"+suffix+".fifo"
	mk := func(name string, attrs map[string]string) string {
		attrs["FifoQueue"] = "true"
		out, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			t.Fatalf("create queue %s: %v", name, err)
		}
		url := aws.ToString(out.QueueUrl)
		t.Cleanup(func() { _, _ = c.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}) })
		return url
	}
	dlqURL := mk(dlq, map[string]string{})
	attrs, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": fmt.Sprint(maxReceive)})
	mk(in, map[string]string{"VisibilityTimeout": fmt.Sprint(int(visibility.Seconds())), "RedrivePolicy": string(redrive)})
	mk(ev, map[string]string{})
	cfg := config.SQSConfig{
		ConsumerEnabled: true, ConsumerName: "it-consumer-" + suffix,
		InboundQueue: in, InboundDLQ: dlq, EventsQueue: ev,
		Pollers: 2, MaxMessages: 10, WaitTime: time.Second,
		VisibilityTimeout: visibility, HandlerTimeout: visibility - time.Second,
		RetryBaseDelay: time.Second, RetryMaxDelay: 2 * time.Second,
		AllowedProviders: []string{"provider-a", "provider-b"},
	}
	q := messaging.NewQueues(c, cfg)
	if err := q.Resolve(ctx, 10*time.Second, Logger()); err != nil {
		t.Fatal(err)
	}
	return TestQueues{Config: cfg, Queues: q}
}

// InboundBody renderiza um corpo de mensagem WagerTransactionRequested.
func InboundBody(messageID string, raw domain.RawExternalRequest) string {
	data := map[string]any{
		"providerId":            raw.ProviderID,
		"externalTransactionId": raw.ExternalTransactionID,
		"idempotencyKey":        raw.IdempotencyKey,
		"playerId":              raw.PlayerID,
		"walletId":              raw.WalletID,
		"roundId":               raw.RoundID,
		"gameId":                raw.GameID,
		"kind":                  raw.Kind,
		"money":                 map[string]string{"amount": raw.Amount, "currency": raw.Currency},
	}
	if raw.ReferenceExternalTransactionID != "" {
		data["referenceExternalTransactionId"] = raw.ReferenceExternalTransactionID
	}
	b, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	})
	return string(b)
}

// SendInbound envia uma mensagem com group = walletId, dedup = messageId.
func SendInbound(t testing.TB, c *sqs.Client, queueURL, messageID string, raw domain.RawExternalRequest) {
	t.Helper()
	SendRaw(t, c, queueURL, raw.WalletID, messageID, InboundBody(messageID, raw))
}

// SendRaw envia um corpo arbitrário.
func SendRaw(t testing.TB, c *sqs.Client, queueURL, group, dedup, body string) {
	t.Helper()
	_, err := c.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
}

// CountMessages drena uma fila (short polling) e retorna os corpos.
func CountMessages(t testing.TB, c *sqs.Client, queueURL string, wait time.Duration) []string {
	t.Helper()
	var bodies []string
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			bodies = append(bodies, aws.ToString(m.Body))
		}
	}
	return bodies
}

// --- Keycloak --------------------------------------------------------------------

// Token obtém um token de acesso com o grant client_credentials.
func Token(t testing.TB, clientID, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	var lastErr error
	for i := 0; i < 30; i++ {
		resp, err := http.PostForm(TokenURL, form)
		if err == nil {
			var body struct {
				AccessToken string `json:"access_token"`
				Error       string `json:"error"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && body.AccessToken != "" {
				return body.AccessToken
			}
			lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, body.Error)
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("token for %s: %v (is keycloak running?)", clientID, lastErr)
	return ""
}

// Identidades de teste bem conhecidas provisionadas por deploy/keycloak/realm-wagering.json.
const (
	ProviderA          = "provider-a"
	ProviderASecret    = "provider-a-secret"
	ProviderB          = "provider-b"
	ProviderBSecret    = "provider-b-secret"
	WalletService      = "wallet-service"
	WalletServiceSec   = "wallet-service-secret"
	ShortLived         = "provider-a-shortlived"
	ShortLivedSecret   = "provider-a-shortlived-secret"
	NoRoleClient       = "no-role-client"
	NoRoleClientSecret = "no-role-client-secret"
)
