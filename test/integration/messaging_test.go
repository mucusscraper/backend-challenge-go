//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/messaging"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
	"github.com/mucusscraper/backend-challenge-go/internal/postgres"
	"github.com/mucusscraper/backend-challenge-go/internal/worker"
	tu "github.com/mucusscraper/backend-challenge-go/test/testutil"
)

func newConsumer(t *testing.T, s *tu.Services, c *sqs.Client, q tu.TestQueues) *messaging.Consumer {
	return messaging.NewConsumer(c, q.Queues, s.Wagering, q.Config, tu.Logger(), s.Metrics)
}

// receiveOne receives a single message from the inbound queue.
func receiveOne(t *testing.T, c *sqs.Client, q tu.TestQueues, wait time.Duration) (types.Message, bool) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(q.Queues.Inbound), MaxNumberOfMessages: 1, WaitTimeSeconds: 1,
			VisibilityTimeout: int32(q.Config.VisibilityTimeout.Seconds()),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Messages) == 1 {
			return out.Messages[0], true
		}
	}
	return types.Message{}, false
}

// TestConsumerProcessesAndDeletes: the happy path through a real queue with
// the running consumer (fx-like Start/Stop).
func TestConsumerProcessesAndDeletes(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	w := tu.OpenWallet(t, s, "100.00")
	raw := tu.RawReq("provider-a", w, domain.KindBet, "25.00", "sqs-"+uuid.NewString(), "")
	tu.SendInbound(t, c, q.Queues.Inbound, "msg-"+uuid.NewString(), raw)

	cons := newConsumer(t, s, c, q)
	if err := cons.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitBalance(t, s, w.ID(), 7500, 15*time.Second)
	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cons.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if left := tu.CountMessages(t, c, q.Queues.Inbound, 2*time.Second); len(left) != 0 {
		t.Fatalf("message not deleted: %d left", len(left))
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestCrashAfterCommitBeforeDelete: the first consumer commits and "dies"
// before deleting; the message is redelivered to a second consumer, which
// recognises it through the inbox. One debit only.
func TestCrashAfterCommitBeforeDelete(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 5, 2*time.Second)
	w := tu.OpenWallet(t, s, "100.00")
	raw := tu.RawReq("provider-a", w, domain.KindBet, "40.00", "crash-"+uuid.NewString(), "")
	msgID := "msg-" + uuid.NewString()
	tu.SendInbound(t, c, q.Queues.Inbound, msgID, raw)

	crashing := newConsumer(t, s, c, q)
	crashing.AfterCommit = func(string) error { return errors.New("process killed") }
	m, ok := receiveOne(t, c, q, 5*time.Second)
	if !ok {
		t.Fatal("no message")
	}
	if out := crashing.HandleMessage(context.Background(), m); out != messaging.OutcomeRetry {
		t.Fatalf("outcome %s", out)
	}
	waitBalance(t, s, w.ID(), 6000, time.Second)

	// Redelivery after the visibility timeout, to an independent instance.
	other := tu.NewServices(t, domain.DefaultReferencePolicy)
	recovering := newConsumer(t, other, c, q)
	m2, ok := receiveOne(t, c, q, 10*time.Second)
	if !ok {
		t.Fatal("message was not redelivered")
	}
	if rc := m2.Attributes["ApproximateReceiveCount"]; rc != "2" {
		t.Fatalf("receive count %s", rc)
	}
	if out := recovering.HandleMessage(context.Background(), m2); out != messaging.OutcomeAck {
		t.Fatalf("outcome %s", out)
	}
	if n := testutilCounter(other.Metrics, "inbox"); n != 1 {
		t.Fatalf("inbox duplicate metric = %v", n)
	}
	if _, ok := receiveOne(t, c, q, 3*time.Second); ok {
		t.Fatal("message still in queue after ack")
	}
	_, _, debits := tu.LedgerStats(t, s.Pool, w.ID())
	if debits != 1 {
		t.Fatalf("debits %d", debits)
	}
	var inbox int
	_ = s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM inbox_messages WHERE message_id = $1`, msgID).Scan(&inbox)
	if inbox != 1 {
		t.Fatalf("inbox rows %d", inbox)
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestInvalidMessagesGoToDLQ: malformed, forbidden kind (OPENING) and
// unknown providers are permanent errors -> DLQ with reason.
func TestInvalidMessagesGoToDLQ(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	w := tu.OpenWallet(t, s, "100.00")
	cons := newConsumer(t, s, c, q)

	opening := tu.RawReq("provider-a", w, domain.KindOpening, "10.00", "open-"+uuid.NewString(), "")
	unknown := tu.RawReq("provider-z", w, domain.KindBet, "10.00", "unk-"+uuid.NewString(), "")
	tu.SendRaw(t, c, q.Queues.Inbound, "g1", "bad-json-"+uuid.NewString(), "{not json")
	tu.SendInbound(t, c, q.Queues.Inbound, "m-open-"+uuid.NewString(), opening)
	tu.SendInbound(t, c, q.Queues.Inbound, "m-unk-"+uuid.NewString(), unknown)

	for i := 0; i < 3; i++ {
		m, ok := receiveOne(t, c, q, 5*time.Second)
		if !ok {
			t.Fatal("missing message")
		}
		if out := cons.HandleMessage(context.Background(), m); out != messaging.OutcomeDLQ {
			t.Fatalf("message %d outcome %s", i, out)
		}
	}
	dlq := tu.CountMessages(t, c, q.Queues.DLQ, 3*time.Second)
	if len(dlq) != 3 {
		t.Fatalf("dlq has %d messages", len(dlq))
	}
	bal, _ := tu.StoredBalance(t, s.Pool, w.ID())
	if bal != 10000 {
		t.Fatal("invalid messages moved money")
	}
}

// TestTransientFailuresExhaustToDLQ: with the database unavailable every
// attempt fails transiently; the message is retried with backoff and the
// SQS redrive policy moves it to the DLQ after maxReceiveCount.
func TestTransientFailuresExhaustToDLQ(t *testing.T) {
	healthy := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, healthy, "100.00")
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 2, 2*time.Second)
	q.Config.RetryBaseDelay, q.Config.RetryMaxDelay = time.Second, time.Second

	broken := tu.NewServices(t, domain.DefaultReferencePolicy)
	broken.Pool.Close() // PostgreSQL "down" for this instance
	cons := newConsumer(t, broken, c, q)

	tu.SendInbound(t, c, q.Queues.Inbound, "m-"+uuid.NewString(),
		tu.RawReq("provider-a", w, domain.KindBet, "10.00", "tr-"+uuid.NewString(), ""))
	for attempt := 1; attempt <= 2; attempt++ {
		m, ok := receiveOne(t, c, q, 10*time.Second)
		if !ok {
			t.Fatalf("attempt %d: no message", attempt)
		}
		if out := cons.HandleMessage(context.Background(), m); out != messaging.OutcomeRetry {
			t.Fatalf("attempt %d outcome %s", attempt, out)
		}
	}
	// Third receive: SQS moves it to the DLQ instead of delivering it.
	if _, ok := receiveOne(t, c, q, 5*time.Second); ok {
		t.Fatal("message delivered beyond maxReceiveCount")
	}
	if dlq := tu.CountMessages(t, c, q.Queues.DLQ, 3*time.Second); len(dlq) != 1 {
		t.Fatalf("dlq has %d messages", len(dlq))
	}
	bal, _ := tu.StoredBalance(t, healthy.Pool, w.ID())
	if bal != 10000 {
		t.Fatal("failed message moved money")
	}
}

// TestSameOperationThroughHTTPAndSQS: the same operation arrives through
// both entry points concurrently; one debit, both succeed.
func TestSameOperationThroughHTTPAndSQS(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	w := tu.OpenWallet(t, s, "100.00")
	raw := tu.RawReq("provider-a", w, domain.KindBet, "30.00", "cross-"+uuid.NewString(), "")
	req, _ := domain.NewExternalRequest(raw)
	tu.SendInbound(t, c, q.Queues.Inbound, "m-"+uuid.NewString(), raw)
	cons := newConsumer(t, tu.NewServices(t, domain.DefaultReferencePolicy), c, q)
	m, ok := receiveOne(t, c, q, 5*time.Second)
	if !ok {
		t.Fatal("no message")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var httpRes app.SubmitResult
	var outcome messaging.Outcome
	go func() { defer wg.Done(); outcome = cons.HandleMessage(context.Background(), m) }()
	go func() { defer wg.Done(); httpRes, _ = submit(s, req) }()
	wg.Wait()
	if outcome != messaging.OutcomeAck || httpRes.Transaction == nil || httpRes.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("outcome %s http %+v", outcome, httpRes)
	}
	_, _, debits := tu.LedgerStats(t, s.Pool, w.ID())
	bal, _ := tu.StoredBalance(t, s.Pool, w.ID())
	if debits != 1 || bal != 7000 {
		t.Fatalf("debits=%d balance=%d", debits, bal)
	}
}

// TestOutboxCompetingPublishersAndRecovery: two relays share the outbox;
// one crashes between publish and confirmation; the other recovers the
// abandoned events after the lease and republishes them with the same
// eventId. Every event ends up published.
func TestOutboxCompetingPublishersAndRecovery(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	c := tu.SQSClient(t)
	q := tu.CreateQueues(t, c, 3, 5*time.Second)
	// Produce events for this test.
	var walletIDs []string
	for i := 0; i < 5; i++ {
		w := tu.OpenWallet(t, s, "10.00")
		walletIDs = append(walletIDs, w.ID().String())
	}
	cfg := config.OutboxConfig{BatchSize: 200, Lease: 2 * time.Second, BaseBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	pub := messaging.NewPublisher(c, q.Queues)
	storeA := postgres.NewOutboxStore(s.Pool)
	storeB := postgres.NewOutboxStore(tu.NewServices(t, domain.DefaultReferencePolicy).Pool)
	relayA := worker.NewOutboxRelay(storeA, pub, cfg, "relay-A", tu.Logger(), s.Metrics)
	relayB := worker.NewOutboxRelay(storeB, pub, cfg, "relay-B", tu.Logger(), s.Metrics)

	var crashed sync.Map
	relayA.BeforeConfirm = func(m app.OutboxMessage) error {
		crashed.Store(m.ID, true)
		return errors.New("killed between publish and confirm")
	}
	// A claims a batch and "crashes" before confirming.
	if n, err := relayA.RunOnce(context.Background()); err != nil || n == 0 {
		t.Fatalf("relay A: n=%d err=%v", n, err)
	}
	// Both relays keep competing until the outbox is drained.
	relayA.BeforeConfirm = nil
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var wg sync.WaitGroup
		for _, r := range []*worker.OutboxRelay{relayA, relayB} {
			wg.Add(1)
			go func(r *worker.OutboxRelay) { defer wg.Done(); _, _ = r.RunOnce(context.Background()) }(r)
		}
		wg.Wait()
		if unpublished(t, s, walletIDs) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := unpublished(t, s, walletIDs); n != 0 {
		t.Fatalf("%d events still unpublished", n)
	}
	// The crashed events were delivered with their original eventId.
	bodies := tu.CountMessages(t, c, q.Queues.Events, 3*time.Second)
	seen := map[string]int{}
	for _, b := range bodies {
		var env struct {
			EventID string `json:"eventId"`
		}
		_ = json.Unmarshal([]byte(b), &env)
		seen[env.EventID]++
	}
	crashed.Range(func(k, _ any) bool {
		id := k.(uuid.UUID).String()
		if seen[id] == 0 {
			t.Errorf("crashed event %s never delivered", id)
		}
		var attempts int
		_ = s.Pool.QueryRow(context.Background(), `SELECT attempts FROM outbox_events WHERE id = $1`, k).Scan(&attempts)
		if attempts < 2 {
			t.Errorf("crashed event %s attempts=%d, expected a republication", id, attempts)
		}
		return true
	})
}

func unpublished(t *testing.T, s *tu.Services, walletIDs []string) int {
	var n int
	err := s.Pool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbox_events o
		 WHERE published_at IS NULL
		   AND (o.aggregate_id = ANY($1)
		        OR o.aggregate_id IN (SELECT id::text FROM wager_transactions WHERE wallet_id::text = ANY($1)))`,
		walletIDs).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func waitBalance(t *testing.T, s *tu.Services, walletID uuid.UUID, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		bal, _ := tu.StoredBalance(t, s.Pool, walletID)
		if bal == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("balance %d, want %d", bal, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// testutilCounter reads the SQS duplicates counter for a mechanism.
func testutilCounter(m *observability.Metrics, mech string) float64 {
	return promtest.ToFloat64(m.DuplicatesTotal.WithLabelValues("sqs", mech))
}
