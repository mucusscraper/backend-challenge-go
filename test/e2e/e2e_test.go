//go:build e2e

// End-to-end tests against THREE independent service processes (app1, app2,
// app3 of docker compose), each with its own memory and connection pool,
// sharing only PostgreSQL, SQS and Keycloak.
//
//	docker compose up -d --build
//	go test -race -tags e2e -count=1 ./test/e2e/...
//
// Failure simulation (kills app1 with SIGKILL during load, then restarts it):
//
//	E2E_CHAOS=1 go test -tags e2e -count=1 -run Chaos ./test/e2e/...
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	tu "github.com/mucusscraper/backend-challenge-go/test/testutil"
)

var bases = strings.Split(tu.Env("E2E_BASE_URLS", "http://localhost:8081,http://localhost:8082,http://localhost:8083"), ",")

var client = &http.Client{Timeout: 20 * time.Second}

type resp struct {
	status int
	body   map[string]any
}

func do(method, url, token string, body any, headers ...string) (resp, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	r, err := client.Do(req)
	if err != nil {
		return resp{}, err
	}
	defer r.Body.Close()
	out := resp{status: r.StatusCode, body: map[string]any{}}
	_ = json.NewDecoder(r.Body).Decode(&out.body)
	return out, nil
}

func mustDo(t *testing.T, method, url, token string, body any, headers ...string) resp {
	t.Helper()
	r, err := do(method, url, token, body, headers...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type env struct {
	op, pa string
	pool   *pgxpool.Pool
}

func setup(t *testing.T) env {
	t.Helper()
	for _, b := range bases {
		deadline := time.Now().Add(60 * time.Second)
		for {
			r, err := client.Get(b + "/health/ready")
			if err == nil && r.StatusCode == 200 {
				r.Body.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s not ready (docker compose up -d --build)", b)
			}
			time.Sleep(time.Second)
		}
	}
	pool, err := pgxpool.New(context.Background(), tu.OwnerDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return env{op: tu.Token(t, tu.WalletService, tu.WalletServiceSec), pa: tu.Token(t, tu.ProviderA, tu.ProviderASecret), pool: pool}
}

type wallet struct{ id, player string }

func openWallet(t *testing.T, e env, amount string) wallet {
	t.Helper()
	r := mustDo(t, "POST", bases[0]+"/wallets", e.op, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}})
	if r.status != 201 {
		t.Fatalf("open wallet %d %v", r.status, r.body)
	}
	return wallet{id: r.body["id"].(string), player: r.body["playerId"].(string)}
}

func body(w wallet, kind, amount, ext, ref string) map[string]any {
	b := map[string]any{
		"providerId": "provider-a", "externalTransactionId": ext, "playerId": w.player, "walletId": w.id,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		b["referenceExternalTransactionId"] = ref
	}
	return b
}

func stats(t *testing.T, e env, walletID string) (balance, net, debits, entries int64) {
	t.Helper()
	err := e.pool.QueryRow(context.Background(), `
		SELECT w.balance_minor,
		       COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0),
		       COUNT(l.id) FILTER (WHERE l.direction='DEBIT'), COUNT(l.id)
		  FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		 WHERE w.id = $1 GROUP BY w.balance_minor`, walletID).Scan(&balance, &net, &debits, &entries)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func reconcile(t *testing.T, e env, walletID string) {
	t.Helper()
	for _, b := range bases {
		r := mustDo(t, "POST", b+"/wallets/"+walletID+"/reconciliation", e.op, nil)
		if r.status != 200 || r.body["consistent"] != true {
			t.Fatalf("reconciliation via %s: %d %v", b, r.status, r.body)
		}
	}
}

// TestE2ESameBetFiftyTimesAcrossInstances: 50 parallel deliveries spread
// over three processes -> a single debit.
func TestE2ESameBetFiftyTimesAcrossInstances(t *testing.T) {
	e := setup(t)
	w := openWallet(t, e, "100.00")
	ext := "e2e-same-" + uuid.NewString()
	var wg sync.WaitGroup
	var fresh, replays atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := mustDo(t, "POST", bases[i%3]+"/wagering/transactions", e.pa, body(w, "BET", "25.00", ext, ""), "Idempotency-Key", "provider-a:"+ext)
			if r.status != 200 || r.body["balance"].(map[string]any)["amount"] != "75.00" {
				t.Errorf("status %d %v", r.status, r.body)
				return
			}
			if r.body["idempotentReplay"] == true {
				replays.Add(1)
			} else {
				fresh.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if fresh.Load() != 1 || replays.Load() != 49 {
		t.Fatalf("fresh=%d replays=%d", fresh.Load(), replays.Load())
	}
	bal, net, debits, _ := stats(t, e, w.id)
	if bal != 7500 || net != 7500 || debits != 1 {
		t.Fatalf("balance=%d net=%d debits=%d", bal, net, debits)
	}
	reconcile(t, e, w.id)
}

// TestE2ETwoBetsRaceAcrossInstances: the mandatory 100.00 / 2x80.00 race,
// each bet sent to a different process.
func TestE2ETwoBetsRaceAcrossInstances(t *testing.T) {
	e := setup(t)
	for round := 0; round < 10; round++ {
		w := openWallet(t, e, "100.00")
		exts := []string{"e2e-race-a-" + uuid.NewString(), "e2e-race-b-" + uuid.NewString()}
		results := make([]resp, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, ext := range exts {
			wg.Add(1)
			go func(i int, ext string) {
				defer wg.Done()
				<-start
				results[i] = mustDo(t, "POST", bases[i]+"/wagering/transactions", e.pa, body(w, "BET", "80.00", ext, ""), "Idempotency-Key", "provider-a:"+ext)
			}(i, ext)
		}
		close(start)
		wg.Wait()
		codes := map[int]int{}
		for _, r := range results {
			codes[r.status]++
			if r.status == 422 && r.body["failureCode"] != "INSUFFICIENT_FUNDS" {
				t.Fatalf("rejection %v", r.body)
			}
		}
		if codes[200] != 1 || codes[422] != 1 {
			t.Fatalf("round %d statuses %v", round, codes)
		}
		// Resends through the third instance change nothing.
		for i, ext := range exts {
			r := mustDo(t, "POST", bases[2]+"/wagering/transactions", e.pa, body(w, "BET", "80.00", ext, ""), "Idempotency-Key", "provider-a:"+ext)
			if r.status != results[i].status || r.body["idempotentReplay"] != true {
				t.Fatalf("resend %d: %d %v", i, r.status, r.body)
			}
		}
		bal, net, debits, _ := stats(t, e, w.id)
		if bal != 2000 || net != 2000 || debits != 1 {
			t.Fatalf("balance=%d net=%d debits=%d", bal, net, debits)
		}
	}
}

// TestE2EDistinctWalletsInParallel: many wallets, many operations, three
// processes; all complete and stay consistent.
func TestE2EDistinctWalletsInParallel(t *testing.T) {
	e := setup(t)
	wallets := make([]wallet, 20)
	for i := range wallets {
		wallets[i] = openWallet(t, e, "100.00")
	}
	var wg sync.WaitGroup
	for i, w := range wallets {
		for j := 0; j < 5; j++ {
			wg.Add(1)
			go func(i, j int, w wallet) {
				defer wg.Done()
				ext := fmt.Sprintf("e2e-par-%s-%d", w.id, j)
				r := mustDo(t, "POST", bases[(i+j)%3]+"/wagering/transactions", e.pa, body(w, "BET", "10.00", ext, ""), "Idempotency-Key", ext)
				if r.status != 200 {
					t.Errorf("wallet %d op %d: %d %v", i, j, r.status, r.body)
				}
			}(i, j, w)
		}
	}
	wg.Wait()
	for _, w := range wallets {
		bal, net, debits, _ := stats(t, e, w.id)
		if bal != 5000 || net != 5000 || debits != 5 {
			t.Fatalf("wallet %s balance=%d net=%d debits=%d", w.id, bal, net, debits)
		}
	}
}

// TestE2EReversalBeforeReferenceAcrossInstances: REFUND sent to app1
// before its BET (sent to app2); any instance resolves it.
func TestE2EReversalBeforeReferenceAcrossInstances(t *testing.T) {
	e := setup(t)
	w := openWallet(t, e, "100.00")
	betExt, refundExt := "e2e-bet-"+uuid.NewString(), "e2e-refund-"+uuid.NewString()
	r := mustDo(t, "POST", bases[0]+"/wagering/transactions", e.pa, body(w, "REFUND", "30.00", refundExt, betExt), "Idempotency-Key", refundExt)
	if r.status != 202 || r.body["status"] != "PENDING_REFERENCE" {
		t.Fatalf("refund %d %v", r.status, r.body)
	}
	txID := r.body["transactionId"].(string)
	if b := mustDo(t, "POST", bases[1]+"/wagering/transactions", e.pa, body(w, "BET", "30.00", betExt, ""), "Idempotency-Key", betExt); b.status != 200 {
		t.Fatalf("bet %d", b.status)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		g := mustDo(t, "GET", bases[2]+"/wagering/transactions/"+txID, e.pa, nil)
		if g.body["status"] == "PROCESSED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refund not resolved: %v", g.body)
		}
		time.Sleep(200 * time.Millisecond)
	}
	bal, _, _, _ := stats(t, e, w.id)
	if bal != 10000 {
		t.Fatalf("balance %d", bal)
	}
	reconcile(t, e, w.id)
}

func sqsClient(t *testing.T) *sqs.Client { return tu.SQSClient(t) }

func queueURL(t *testing.T, c *sqs.Client, name string) string {
	out, err := c.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatal(err)
	}
	return aws.ToString(out.QueueUrl)
}

// TestE2EHTTPAndSQSSameOperation: the same operation through the real
// inbound queue (consumed by the three instances) and HTTP -> one debit.
// The message is also sent several times with different broker dedup ids
// so that the application inbox/idempotency is what deduplicates.
func TestE2EHTTPAndSQSSameOperation(t *testing.T) {
	e := setup(t)
	c := sqsClient(t)
	inbound := queueURL(t, c, "wager-transactions.fifo")
	w := openWallet(t, e, "100.00")
	ext := "e2e-cross-" + uuid.NewString()
	wid, _ := uuid.Parse(w.id)
	pid, _ := uuid.Parse(w.player)
	wal, _ := domain.RehydrateWallet(domain.WalletSnapshot{ID: wid, PlayerID: pid, Balance: tu.BRL(t, "100.00"), Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	raw := tu.RawReq("provider-a", wal, domain.KindBet, "35.00", ext, "")
	msgID := "e2e-msg-" + uuid.NewString()
	for i := 0; i < 3; i++ {
		// Same messageId (inbox dedup), different broker dedup id.
		tu.SendRaw(t, c, inbound, w.id, fmt.Sprintf("%s-%d", msgID, i), tu.InboundBody(msgID, raw))
	}
	// Another messageId for the same operation (idempotency-key dedup).
	tu.SendInbound(t, c, inbound, msgID+"-other", raw)
	r := mustDo(t, "POST", bases[1]+"/wagering/transactions", e.pa, body(w, "BET", "35.00", ext, ""), "Idempotency-Key", "provider-a:"+ext)
	if r.status != 200 {
		t.Fatalf("http %d %v", r.status, r.body)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var inbox int
		_ = e.pool.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages WHERE message_id IN ($1, $2)`, msgID, msgID+"-other").Scan(&inbox)
		if inbox == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("messages not consumed (inbox rows %d)", inbox)
		}
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(2 * time.Second) // let the remaining duplicates be consumed
	bal, net, debits, _ := stats(t, e, w.id)
	if bal != 6500 || net != 6500 || debits != 1 {
		t.Fatalf("balance=%d net=%d debits=%d", bal, net, debits)
	}
	reconcile(t, e, w.id)
}

// TestE2EOutboxPublishes: events of committed operations are published by
// one of the instances (published_at set) with stable eventIds.
func TestE2EOutboxPublishes(t *testing.T) {
	e := setup(t)
	w := openWallet(t, e, "50.00")
	ext := "e2e-outbox-" + uuid.NewString()
	mustDo(t, "POST", bases[2]+"/wagering/transactions", e.pa, body(w, "BET", "5.00", ext, ""), "Idempotency-Key", ext)
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pending, total int
		_ = e.pool.QueryRow(context.Background(), `
			SELECT count(*) FILTER (WHERE published_at IS NULL), count(*) FROM outbox_events
			 WHERE aggregate_id = $1 OR aggregate_id IN (SELECT id::text FROM wager_transactions WHERE wallet_id = $1::uuid)`,
			w.id).Scan(&pending, &total)
		if total == 4 && pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox: total=%d pending=%d", total, pending)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestE2EChaosKillInstance kills app1 (SIGKILL) during a burst of
// operations, restarts it, and verifies that every operation was applied
// exactly once. Clients retry on another instance with the same key.
func TestE2EChaosKillInstance(t *testing.T) {
	if os.Getenv("E2E_CHAOS") == "" {
		t.Skip("set E2E_CHAOS=1 to run (uses docker compose kill/start)")
	}
	e := setup(t)
	w := openWallet(t, e, "1000.00")
	const ops = 60
	var wg sync.WaitGroup
	killed := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		out, err := exec.Command("docker", "compose", "kill", "-s", "SIGKILL", "app1").CombinedOutput()
		if err != nil {
			t.Errorf("kill: %v %s", err, out)
		}
		close(killed)
	}()
	for i := 0; i < ops; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ext := fmt.Sprintf("e2e-chaos-%s-%d", w.id, i)
			for attempt := 0; ; attempt++ {
				r, err := do("POST", bases[(i+attempt)%3]+"/wagering/transactions", e.pa, body(w, "BET", "10.00", ext, ""), "Idempotency-Key", ext)
				if err == nil && r.status == 200 {
					return
				}
				if err == nil && r.status != 503 && r.status < 500 {
					t.Errorf("op %d: %d %v", i, r.status, r.body)
					return
				}
				if attempt > 20 {
					t.Errorf("op %d never succeeded: %v", i, errors.Join(err))
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	<-killed
	if out, err := exec.Command("docker", "compose", "start", "app1").CombinedOutput(); err != nil {
		t.Fatalf("start: %v %s", err, out)
	}
	bal, net, debits, _ := stats(t, e, w.id)
	if bal != 100000-ops*1000 || net != bal || debits != ops {
		t.Fatalf("balance=%d net=%d debits=%d", bal, net, debits)
	}
}
