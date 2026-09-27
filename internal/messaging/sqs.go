// Package messaging adapta o AWS SQS (LocalStack localmente): o consumer de
// wager-transactions de entrada e o publicador de eventos de saída usado pelo
// outbox relay.
//
// Contratos de roteamento:
//   - entrada  wager-transactions.fifo: MessageGroupId = walletId (ordena as
//     mensagens de uma carteira e permite que carteiras diferentes procedam em
//     paralelo); MessageDeduplicationId = messageId do envelope (dedup de 5 min
//     do broker, apenas otimização: o inbox é o dedup durável).
//   - saída wallet-events.fifo: MessageGroupId = aggregateId,
//     MessageDeduplicationId = eventId, atributos de mensagem eventType e
//     eventId. Consumidores devem deduplicar por eventId (at-least-once).
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

// API é o subconjunto do cliente SQS usado pelo serviço (mockável).
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	GetQueueUrl(ctx context.Context, in *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// NewClient constrói um cliente SQS. As credenciais são estáticas (de variáveis
// de ambiente) para que as políticas de acesso do broker se apliquem a um
// principal conhecido; o endpoint é sobrescrito para o LocalStack.
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

// Queues mantém as URLs de fila resolvidas.
type Queues struct {
	api                  API
	names                config.SQSConfig
	Inbound, DLQ, Events string
}

// NewQueues constrói um Queues não resolvido; chame Resolve na inicialização.
func NewQueues(api API, cfg config.SQSConfig) *Queues {
	return &Queues{api: api, names: cfg}
}

// Resolve consulta as URLs das filas, repetindo até o timeout para que o
// serviço possa iniciar enquanto o broker ainda está provisionando.
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

// Ping verifica se o broker está acessível (readiness).
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

// Publisher envia eventos do outbox para a fila de eventos.
type Publisher struct {
	api    API
	queues *Queues
}

// NewPublisher constrói o publisher.
func NewPublisher(api API, queues *Queues) *Publisher { return &Publisher{api: api, queues: queues} }

var _ app.EventPublisher = (*Publisher)(nil)

// Publish envia o snapshot imutável do envelope. O eventId é o id de
// deduplicação FIFO, portanto uma republicação dentro de 5 minutos é
// descartada pelo broker; após isso, carrega o mesmo eventId para dedup
// no lado do consumidor.
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
