package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// Outcome of handling one message.
type Outcome string

// Handling outcomes.
const (
	// OutcomeAck: durable handling committed (processed, rejected, pending
	// or duplicate); the message is deleted.
	OutcomeAck Outcome = "ack"
	// OutcomeRetry: transient failure; visibility is set to a backoff delay
	// and SQS redelivers. After maxReceiveCount the redrive policy moves the
	// message to the DLQ.
	OutcomeRetry Outcome = "retry"
	// OutcomeDLQ: permanent failure; the message is copied to the DLQ with
	// the reason and deleted from the source queue.
	OutcomeDLQ Outcome = "dlq"
	// OutcomeRelease: shutdown interrupted handling; visibility is reset to
	// 0 so another instance picks it up immediately.
	OutcomeRelease Outcome = "release"
)

// Consumer long-polls the inbound queue and hands messages to the shared
// WageringService use case.
//
// Guarantees:
//   - a message is deleted only after the SQL transaction containing its
//     inbox record and financial effects has committed;
//   - a redelivery (crash after commit, before delete) hits the inbox and is
//     acknowledged without reapplying anything;
//   - on shutdown, polling stops first; in-flight messages get until the
//     stop deadline to finish, otherwise their visibility is released.
type Consumer struct {
	api     API
	queues  *Queues
	svc     *app.WageringService
	cfg     config.SQSConfig
	log     *slog.Logger
	metrics *observability.Metrics

	// AfterCommit is a test hook called after Submit committed and before
	// the message is deleted. Returning an error simulates a crash at that
	// exact point (the message is left in flight and will be redelivered).
	AfterCommit func(messageID string) error

	mu         sync.Mutex
	pollCancel context.CancelFunc
	workCancel context.CancelFunc
	wg         sync.WaitGroup
	started    bool
}

// NewConsumer builds the consumer.
func NewConsumer(api API, queues *Queues, svc *app.WageringService, cfg config.SQSConfig, log *slog.Logger,
	metrics *observability.Metrics) *Consumer {
	return &Consumer{api: api, queues: queues, svc: svc, cfg: cfg, log: log.With("worker", "sqs-consumer"), metrics: metrics}
}

// Start launches cfg.Pollers polling goroutines.
func (c *Consumer) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("consumer already started")
	}
	pollCtx, pollCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())
	c.pollCancel, c.workCancel, c.started = pollCancel, workCancel, true
	for i := 0; i < c.cfg.Pollers; i++ {
		c.wg.Add(1)
		go c.poll(pollCtx, workCtx)
	}
	c.log.Info("sqs consumer started", "queue", c.cfg.InboundQueue, "pollers", c.cfg.Pollers)
	return nil
}

// Stop stops fetching new messages and waits for in-flight handling. If ctx
// expires first, in-flight handlers are cancelled (their transactions roll
// back) and their messages are released for immediate redelivery.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	c.pollCancel()
	c.mu.Unlock()

	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()
	select {
	case <-done:
		c.workCancel()
		c.log.Info("sqs consumer stopped gracefully")
		return nil
	case <-ctx.Done():
		c.workCancel()
		// Give cancelled handlers a moment to release visibility.
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		c.log.Warn("sqs consumer stop deadline reached; in-flight messages released")
		return ctx.Err()
	}
}

func (c *Consumer) poll(pollCtx, workCtx context.Context) {
	defer c.wg.Done()
	backoff := 500 * time.Millisecond
	for pollCtx.Err() == nil {
		out, err := c.api.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queues.Inbound),
			MaxNumberOfMessages: c.cfg.MaxMessages,
			WaitTimeSeconds:     int32(c.cfg.WaitTime.Seconds()),
			VisibilityTimeout:   int32(c.cfg.VisibilityTimeout.Seconds()),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
		})
		if err != nil {
			if pollCtx.Err() != nil {
				return
			}
			c.log.Warn("sqs receive failed", "error", err)
			c.metrics.RetriesTotal.WithLabelValues("sqs_receive").Inc()
			select {
			case <-pollCtx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = 500 * time.Millisecond
		c.handleBatch(workCtx, out.Messages)
	}
}

// handleBatch processes messages of the same MessageGroupId sequentially
// (preserving FIFO order per wallet) and different groups concurrently.
func (c *Consumer) handleBatch(ctx context.Context, msgs []types.Message) {
	groups := map[string][]types.Message{}
	var order []string
	for _, m := range msgs {
		g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], m)
	}
	var wg sync.WaitGroup
	for _, g := range order {
		wg.Add(1)
		go func(batch []types.Message) {
			defer wg.Done()
			for _, m := range batch {
				c.HandleMessage(ctx, m)
			}
		}(groups[g])
	}
	wg.Wait()
}

// HandleMessage processes one message end to end and applies the outcome
// (delete / retry with backoff / DLQ / release). Exported for tests.
func (c *Consumer) HandleMessage(workCtx context.Context, m types.Message) Outcome {
	receivedAt := time.Now().UTC()
	receiveCount, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	ctx := observability.WithLogFields(workCtx, "sqsMessageId", aws.ToString(m.MessageId), "receiveCount", receiveCount)

	outcome, reason, err := c.process(ctx, m, receivedAt)
	if outcome == OutcomeRetry && workCtx.Err() != nil {
		outcome = OutcomeRelease
	}
	// Broker bookkeeping must not be cancelled by shutdown.
	bk, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	switch outcome {
	case OutcomeAck:
		if c.AfterCommit != nil {
			if herr := c.AfterCommit(aws.ToString(m.MessageId)); herr != nil {
				c.log.WarnContext(ctx, "simulated crash after commit, before delete", "error", herr)
				return OutcomeRetry
			}
		}
		c.delete(bk, m)
	case OutcomeDLQ:
		c.toDLQ(bk, m, reason, err)
	case OutcomeRetry:
		delay := c.retryDelay(receiveCount)
		c.metrics.RetriesTotal.WithLabelValues("sqs").Inc()
		c.log.WarnContext(ctx, "transient failure, message will be retried", "error", err, "delay", delay)
		c.changeVisibility(bk, m, delay)
	case OutcomeRelease:
		c.log.WarnContext(ctx, "handling interrupted by shutdown, releasing message", "error", err)
		c.changeVisibility(bk, m, 0)
	}
	return outcome
}

// process maps the use-case result to an outcome.
func (c *Consumer) process(ctx context.Context, m types.Message, receivedAt time.Time) (Outcome, string, error) {
	parsed, err := ParseInbound(aws.ToString(m.Body))
	if err != nil {
		return OutcomeDLQ, "invalid_message", err
	}
	ctx = observability.WithLogFields(ctx, "messageId", parsed.Envelope.MessageID)
	if !slices.Contains(c.cfg.AllowedProviders, parsed.Raw.ProviderID) {
		return OutcomeDLQ, "provider_not_allowed", fmt.Errorf("provider %q is not allowed on this queue", parsed.Raw.ProviderID)
	}
	req, err := domain.NewExternalRequest(parsed.Raw)
	if err != nil {
		return OutcomeDLQ, "validation", err
	}
	correlationID := parsed.Envelope.CorrelationID
	if correlationID == "" {
		correlationID = parsed.Envelope.MessageID
	}
	hctx, cancel := context.WithTimeout(ctx, c.cfg.HandlerTimeout)
	defer cancel()
	_, err = c.svc.Submit(hctx, app.SubmitCommand{
		Request:       req,
		CorrelationID: correlationID,
		Source:        "sqs",
		Message: &app.InboundMessage{
			Consumer:    c.cfg.ConsumerName,
			MessageID:   parsed.Envelope.MessageID,
			PayloadHash: MessageHash(parsed.Envelope.Type, req),
			ReceivedAt:  receivedAt,
		},
	})
	switch {
	case err == nil:
		// PROCESSED, REJECTED (terminal business outcome), PENDING_REFERENCE
		// (the worker takes over) or duplicate: all durable -> delete.
		return OutcomeAck, "", nil
	case errors.Is(err, app.ErrIdempotencyConflict):
		return OutcomeDLQ, "idempotency_conflict", err
	case errors.Is(err, app.ErrExternalIDConflict):
		return OutcomeDLQ, "external_id_conflict", err
	case errors.Is(err, app.ErrMessageConflict):
		return OutcomeDLQ, "message_id_conflict", err
	case errors.Is(err, app.ErrWalletNotFound):
		return OutcomeDLQ, "wallet_not_found", err
	case errors.Is(err, domain.ErrInvalidArgument):
		return OutcomeDLQ, "validation", err
	default:
		// Transient (DB down, lock timeout) or unknown: retry with backoff;
		// the redrive policy bounds the attempts and ends in the DLQ.
		return OutcomeRetry, "", err
	}
}

// retryDelay is exponential in the receive count, capped.
func (c *Consumer) retryDelay(receiveCount int) time.Duration {
	d := c.cfg.RetryBaseDelay
	for i := 1; i < receiveCount && d < c.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, c.cfg.RetryMaxDelay)
}

func (c *Consumer) delete(ctx context.Context, m types.Message) {
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.queues.Inbound), ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		// Safe: the redelivery will hit the inbox and be acknowledged.
		c.log.WarnContext(ctx, "delete failed; message will be redelivered and deduplicated", "error", err)
	}
}

func (c *Consumer) changeVisibility(ctx context.Context, m types.Message, d time.Duration) {
	_, err := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Inbound), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d.Seconds()),
	})
	if err != nil {
		c.log.WarnContext(ctx, "change visibility failed; default visibility timeout applies", "error", err)
	}
}

// toDLQ copies a permanently failing message to the DLQ (with the failure
// reason as attributes) and deletes it from the source queue. If the process
// dies between both calls the message is redelivered and copied again; the
// DLQ deduplication id (source SQS MessageId) absorbs the duplicate.
func (c *Consumer) toDLQ(ctx context.Context, m types.Message, reason string, cause error) {
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	detail := ""
	if cause != nil {
		detail = cause.Error()
		if len(detail) > 256 {
			detail = detail[:256]
		}
	}
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DLQ),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"failureDetail": {DataType: aws.String("String"), StringValue: aws.String(orDash(detail))},
		},
	})
	if err != nil {
		c.log.ErrorContext(ctx, "could not send to DLQ; message will be retried", "error", err)
		return
	}
	c.metrics.DLQTotal.WithLabelValues(reason).Inc()
	c.log.WarnContext(ctx, "message moved to DLQ", "reason", reason, "error", cause)
	c.delete(ctx, m)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
