package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// OutboxRelay publishes committed outbox events to the broker.
//
// Delivery is at-least-once:
//   - crash after commit, before publish: the row stays unpublished and is
//     claimed again (by this or another instance) once due / lease expired;
//   - crash after publish, before confirmation: the lease expires, the row is
//     republished with the SAME eventId (stored in the snapshot), and the
//     broker (FIFO MessageDeduplicationId = eventId) and consumers dedupe.
type OutboxRelay struct {
	store     app.OutboxStore
	publisher app.EventPublisher
	cfg       config.OutboxConfig
	owner     string
	log       *slog.Logger
	metrics   *observability.Metrics

	// BeforeConfirm is a test hook invoked after a successful publish and
	// before the confirmation is written. Returning an error simulates a
	// crash between publication and confirmation.
	BeforeConfirm func(m app.OutboxMessage) error
}

// NewOutboxRelay builds the relay; owner identifies the instance in leases.
func NewOutboxRelay(store app.OutboxStore, publisher app.EventPublisher, cfg config.OutboxConfig, owner string,
	log *slog.Logger, metrics *observability.Metrics) *OutboxRelay {
	return &OutboxRelay{store: store, publisher: publisher, cfg: cfg, owner: owner, log: log.With("worker", "outbox"), metrics: metrics}
}

// RunOnce claims and publishes one batch. It returns how many events were
// claimed.
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
			// Remaining claimed rows are recovered when the lease expires.
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
	// Bookkeeping uses a context detached from shutdown so a successful
	// publication is confirmed even while the worker is stopping.
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

// backoff is exponential in the number of attempts, capped at MaxBackoff.
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

// NewOutboxLoop wraps the relay into a Loop.
func NewOutboxLoop(r *OutboxRelay, cfg config.OutboxConfig, log *slog.Logger) *Loop {
	return NewLoop("outbox-relay", cfg.PollInterval, log, r.RunOnce)
}

// NewPendingLoop runs the pending-reference resolver periodically.
func NewPendingLoop(svc *app.WageringService, cfg config.PendingConfig, log *slog.Logger) *Loop {
	return NewLoop("pending-references", cfg.PollInterval, log, func(ctx context.Context) (int, error) {
		return svc.ResumeDue(ctx, cfg.BatchSize)
	})
}
