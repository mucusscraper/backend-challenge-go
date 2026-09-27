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

// Outcome é o resultado do processamento de uma mensagem.
type Outcome string

// Resultados possíveis de processamento.
const (
	// OutcomeAck: processamento durável confirmado (processado, rejeitado, pendente
	// ou duplicado); a mensagem é deletada.
	OutcomeAck Outcome = "ack"
	// OutcomeRetry: falha transiente; a visibilidade é definida com um atraso de
	// backoff e o SQS reenvia. Após maxReceiveCount, a redrive policy move a
	// mensagem para a DLQ.
	OutcomeRetry Outcome = "retry"
	// OutcomeDLQ: falha permanente; a mensagem é copiada para a DLQ com
	// o motivo e deletada da fila de origem.
	OutcomeDLQ Outcome = "dlq"
	// OutcomeRelease: o desligamento interrompeu o processamento; a visibilidade
	// é redefinida para 0 para que outra instância a processe imediatamente.
	OutcomeRelease Outcome = "release"
)

// Consumer faz long-polling da fila de entrada e repassa mensagens ao caso de
// uso compartilhado WageringService.
//
// Garantias:
//   - uma mensagem só é deletada após o commit da transação SQL que contém
//     seu registro de inbox e os efeitos financeiros;
//   - uma reentrega (crash após commit, antes do delete) acessa o inbox e é
//     confirmada sem reaplicar nada;
//   - no desligamento, o polling para primeiro; mensagens em andamento têm
//     até o prazo de stop para terminar; caso contrário, sua visibilidade é liberada.
type Consumer struct {
	api     API
	queues  *Queues
	svc     *app.WageringService
	cfg     config.SQSConfig
	log     *slog.Logger
	metrics *observability.Metrics

	// AfterCommit é um hook de teste chamado após o commit do Submit e antes
	// da deleção da mensagem. Retornar um erro simula um crash nesse ponto
	// exato (a mensagem fica em andamento e será reenviada).
	AfterCommit func(messageID string) error

	mu         sync.Mutex
	pollCancel context.CancelFunc
	workCancel context.CancelFunc
	wg         sync.WaitGroup
	started    bool
}

// NewConsumer constrói o consumer.
func NewConsumer(api API, queues *Queues, svc *app.WageringService, cfg config.SQSConfig, log *slog.Logger,
	metrics *observability.Metrics) *Consumer {
	return &Consumer{api: api, queues: queues, svc: svc, cfg: cfg, log: log.With("worker", "sqs-consumer"), metrics: metrics}
}

// Start lança cfg.Pollers goroutines de polling.
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

// Stop para de buscar novas mensagens e aguarda o processamento em andamento.
// Se ctx expirar primeiro, os handlers em andamento são cancelados (suas
// transações fazem rollback) e suas mensagens são liberadas para reentrega imediata.
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
		// Dá um momento para os handlers cancelados liberarem a visibilidade.
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

// handleBatch processa mensagens do mesmo MessageGroupId sequencialmente
// (preservando a ordem FIFO por carteira) e grupos diferentes de forma concorrente.
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

// HandleMessage processa uma mensagem de ponta a ponta e aplica o resultado
// (delete / retry com backoff / DLQ / release). Exportado para testes.
func (c *Consumer) HandleMessage(workCtx context.Context, m types.Message) Outcome {
	receivedAt := time.Now().UTC()
	receiveCount, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	ctx := observability.WithLogFields(workCtx, "sqsMessageId", aws.ToString(m.MessageId), "receiveCount", receiveCount)

	outcome, reason, err := c.process(ctx, m, receivedAt)
	if outcome == OutcomeRetry && workCtx.Err() != nil {
		outcome = OutcomeRelease
	}
	// Operações de broker não devem ser canceladas pelo desligamento.
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

// process mapeia o resultado do caso de uso para um outcome.
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
		// PROCESSED, REJECTED (resultado de negócio terminal), PENDING_REFERENCE
		// (o worker assume) ou duplicado: todos duráveis -> delete.
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
		// Transiente (BD indisponível, timeout de lock) ou desconhecido: retry com backoff;
		// a redrive policy limita as tentativas e termina na DLQ.
		return OutcomeRetry, "", err
	}
}

// retryDelay é exponencial em relação ao receiveCount, com limite máximo.
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
		// Seguro: a reentrega acessará o inbox e será confirmada.
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

// toDLQ copia uma mensagem com falha permanente para a DLQ (com o motivo da
// falha como atributos) e a deleta da fila de origem. Se o processo morrer
// entre as duas chamadas, a mensagem é reenviada e copiada novamente; o id de
// deduplicação da DLQ (MessageId SQS de origem) absorve a duplicata.
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
