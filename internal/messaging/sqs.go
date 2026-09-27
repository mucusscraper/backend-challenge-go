// Package messaging adapts AWS SQS (LocalStack locally): the inbound
// wager-transactions consumer and the outbound event publisher used by the
// outbox relay.
//
// Routing contracts:
//   - inbound  wager-transactions.fifo: MessageGroupId = walletId (orders a
//     wallet's messages and lets different wallets proceed in parallel);
//     MessageDeduplicationId = envelope messageId (5-minute broker dedup, an
//     optimisation only: the inbox is the durable dedup).
//   - outbound wallet-events.fifo: MessageGroupId = aggregateId,
//     MessageDeduplicationId = eventId, message attributes eventType and
//     eventId. Consumers must deduplicate by eventId (at-least-once).
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
)

// API is the subset of the SQS client used by the service (mockable).
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// NewClient builds an SQS client. Credentials are static (from env) so the
// broker's access policies apply to a known principal; the endpoint is
// overridden for LocalStack.
func NewClient(cfg config.AWSConfig) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// Queues holds the resolved queue URLs.
type Queues struct {
	api                  API
	names                config.SQSConfig
	Inbound, DLQ, Events string
}

// NewQueues builds an unresolved Queues; call Resolve at startup.
func NewQueues(api API, cfg config.SQSConfig) *Queues {
	return &Queues{api: api, names: cfg}
}

// Resolve looks up queue URLs, retrying until timeout so the service can
// start while the broker is still provisioning.
func (q *Queues) Resolve(ctx context.Context, timeout time.Duration, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := q.resolveOnce(ctx)
		if err == nil {
			return nil
		}
		log.WarnContext(ctx, "queues not available yet", "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("sqs: queues not available: %w", err)
		case <-time.After(time.Second):
		}
	}
}

func (q *Queues) resolveOnce(ctx context.Context) error {
	var err error
	if q.Inbound, err = q.url(ctx, q.names.InboundQueue); err != nil {
		return err
	}
	if q.DLQ, err = q.url(ctx, q.names.InboundDLQ); err != nil {
		return err
	}
	q.Events, err = q.url(ctx, q.names.EventsQueue)
	return err
}

func (q *Queues) url(ctx context.Context, name string) (string, error) {
	out, err := q.api.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("get queue url %s: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

// Ping checks the broker is reachable (readiness).
func (q *Queues) Ping(ctx context.Context) error {
	if q.Inbound == "" {
		return errors.New("sqs: queues not resolved")
	}
	_, err := q.api.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(q.Inbound),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}

// Publisher sends outbox events to the events queue.
type Publisher struct {
	api    API
	queues *Queues
}

// NewPublisher builds the publisher.
func NewPublisher(api API, queues *Queues) *Publisher { return &Publisher{api: api, queues: queues} }

var _ app.EventPublisher = (*Publisher)(nil)

// Publish sends the immutable envelope snapshot. The eventId is the FIFO
// deduplication id, so a republication within 5 minutes is dropped by the
// broker, and after that it carries the same eventId for consumer dedup.
func (p *Publisher) Publish(ctx context.Context, m app.OutboxMessage) error {
	_, err := p.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queues.Events),
		MessageBody:            aws.String(m.Payload),
		MessageGroupId:         aws.String(m.AggregateID),
		MessageDeduplicationId: aws.String(m.ID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(m.EventType)},
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(m.ID.String())},
		},
	})
	return err
}
