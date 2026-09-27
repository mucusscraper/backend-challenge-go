package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// OutboxRelay publica eventos de outbox confirmados para o broker.
//
// A entrega é at-least-once:
//   - crash após commit, antes da publicação: a linha fica não publicada e é
//     reivindicada novamente (por esta ou outra instância) quando devida / lease expirado;
//   - crash após publicação, antes da confirmação: o lease expira, a linha é
//     republicada com o MESMO eventId (armazenado no snapshot) e o broker
//     (FIFO MessageDeduplicationId = eventId) e os consumidores deduplicam.
type OutboxRelay struct {
	store     app.OutboxStore
	publisher app.EventPublisher
	cfg       config.OutboxConfig
	owner     string
	log       *slog.Logger
	metrics   *observability.Metrics

	// BeforeConfirm é um hook de teste invocado após publicação bem-sucedida e
	// antes da confirmação ser gravada. Retornar um erro simula um crash entre
	// a publicação e a confirmação.
	BeforeConfirm func(m app.OutboxMessage) error
}

// NewOutboxRelay constrói o relay; owner identifica a instância nos leases.
func NewOutboxRelay(store app.OutboxStore, publisher app.EventPublisher, cfg config.OutboxConfig, owner string,
	log *slog.Logger, metrics *observability.Metrics) *OutboxRelay {
	return &OutboxRelay{store: store, publisher: publisher, cfg: cfg, owner: owner, log: log.With("worker", "outbox"), metrics: metrics}
}

// RunOnce reivindica e publica um lote. Retorna quantos eventos foram reivindicados.
func (r *OutboxRelay) RunOnce(ctx context.Context) (int, error) {
	if lag, err := r.store.Lag(ctx); err == nil {
		r.metrics.OutboxLag.Set(lag.Seconds())
	}
	batch, err := r.store.Claim(ctx, r.owner, r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, m := range batch {
		if ctx.Err() != nil {
			// As linhas reivindicadas restantes são recuperadas quando o lease expira.
			return len(batch), nil
		}
		r.publishOne(ctx, m)
	}
	return len(batch), nil
}

func (r *OutboxRelay) publishOne(ctx context.Context, m app.OutboxMessage) {
	log := r.log.With("eventId", m.ID.String(), "eventType", m.EventType, "aggregateId", m.AggregateID, "attempt", m.Attempts)
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := r.publisher.Publish(pubCtx, m)
	cancel()
	// Operações de bookkeeping usam um contexto desvinculado do desligamento para
	// que uma publicação bem-sucedida seja confirmada mesmo enquanto o worker para.
	bk, bkCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer bkCancel()
	if err != nil {
		r.metrics.OutboxFailuresTotal.Inc()
		r.metrics.RetriesTotal.WithLabelValues("outbox").Inc()
		next := time.Now().Add(r.backoff(m.Attempts))
		if ferr := r.store.MarkFailed(bk, m.ID, r.owner, err.Error(), next); ferr != nil {
			log.Warn("could not record publish failure; lease expiry will retry", "error", ferr)
		}
		log.Warn("outbox publish failed", "error", err, "nextAttemptAt", next)
		return
	}
	if r.BeforeConfirm != nil {
		if herr := r.BeforeConfirm(m); herr != nil {
			log.Warn("simulated crash before outbox confirmation", "error", herr)
			return
		}
	}
	if err := r.store.MarkPublished(bk, m.ID); err != nil {
		log.Warn("publish confirmation failed; event will be republished with the same eventId", "error", err)
		return
	}
	r.metrics.OutboxPublishedTotal.WithLabelValues(m.EventType).Inc()
	log.Debug("outbox event published")
}

// backoff é exponencial em relação ao número de tentativas, limitado por MaxBackoff.
func (r *OutboxRelay) backoff(attempts int) time.Duration {
	d := r.cfg.BaseBackoff
	for i := 1; i < attempts && d < r.cfg.MaxBackoff; i++ {
		d *= 2
	}
	if d > r.cfg.MaxBackoff {
		d = r.cfg.MaxBackoff
	}
	return d
}

// NewOutboxLoop envolve o relay em um Loop.
func NewOutboxLoop(r *OutboxRelay, cfg config.OutboxConfig, log *slog.Logger) *Loop {
	return NewLoop("outbox-relay", cfg.PollInterval, log, r.RunOnce)
}

// NewPendingLoop executa o resolvedor de referências pendentes periodicamente.
func NewPendingLoop(svc *app.WageringService, cfg config.PendingConfig, log *slog.Logger) *Loop {
	return NewLoop("pending-references", cfg.PollInterval, log, func(ctx context.Context) (int, error) {
		return svc.ResumeDue(ctx, cfg.BatchSize)
	})
}
