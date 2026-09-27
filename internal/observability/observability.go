// Package observability provides structured JSON logging (log/slog) and
// Prometheus metrics. Log helpers only carry identifiers (correlationId,
// messageId, transactionId, walletId, providerId); credentials and full
// financial payloads are never logged.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// NewLogger builds a JSON logger writing to stdout.
func NewLogger(level, instanceID string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(&contextHandler{Handler: h}).With("instance", instanceID)
}

type logFieldsKey struct{}

// WithLogFields returns a context carrying extra log attributes. Handlers
// automatically add them to every record logged with that context.
func WithLogFields(ctx context.Context, args ...any) context.Context {
	prev, _ := ctx.Value(logFieldsKey{}).([]any)
	merged := make([]any, 0, len(prev)+len(args))
	merged = append(merged, prev...)
	merged = append(merged, args...)
	return context.WithValue(ctx, logFieldsKey{}, merged)
}

// contextHandler injects the attributes stored by WithLogFields.
type contextHandler struct {
	slog.Handler
}

func (h *contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if fields, ok := ctx.Value(logFieldsKey{}).([]any); ok {
		r.Add(fields...)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *contextHandler) WithGroup(name string) slog.Handler {
	return &contextHandler{Handler: h.Handler.WithGroup(name)}
}

// Metrics groups every Prometheus collector of the service.
type Metrics struct {
	Registry *prometheus.Registry

	// TransactionsTotal counts processed operations by kind, final status
	// and source (http, sqs, worker).
	TransactionsTotal *prometheus.CounterVec
	// DuplicatesTotal counts replays detected by idempotency (idempotency
	// key / external id) or by the inbox (messageId).
	DuplicatesTotal *prometheus.CounterVec
	// RetriesTotal counts retries by component (db_tx, sqs, outbox, pending).
	RetriesTotal *prometheus.CounterVec
	// DLQTotal counts messages sent to the DLQ by reason.
	DLQTotal *prometheus.CounterVec
	// ConcurrencyConflictsTotal counts lock timeouts, unique races and
	// version mismatches resolved by retrying.
	ConcurrencyConflictsTotal *prometheus.CounterVec
	// ProcessingDuration measures use-case latency by source.
	ProcessingDuration *prometheus.HistogramVec
	// OutboxLag is the age in seconds of the oldest unpublished event.
	OutboxLag prometheus.Gauge
	// OutboxPublishedTotal counts published events by type.
	OutboxPublishedTotal *prometheus.CounterVec
	// OutboxFailuresTotal counts failed publish attempts.
	OutboxFailuresTotal prometheus.Counter
	// ReconciliationDivergences counts wallets whose stored balance differs
	// from the ledger.
	ReconciliationDivergences prometheus.Counter
	// PendingResolutions counts pending-reference outcomes.
	PendingResolutions *prometheus.CounterVec
}

// NewMetrics registers all collectors in a dedicated registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{
		Registry: reg,
		TransactionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_transactions_total", Help: "Wager transactions by kind, status and source.",
		}, []string{"kind", "status", "source"}),
		DuplicatesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_duplicates_total", Help: "Duplicate deliveries detected.",
		}, []string{"source", "mechanism"}),
		RetriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_retries_total", Help: "Retries by component.",
		}, []string{"component"}),
		DLQTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_sqs_dlq_total", Help: "Messages moved to the DLQ.",
		}, []string{"reason"}),
		ConcurrencyConflictsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_concurrency_conflicts_total", Help: "Concurrency conflicts resolved by retry.",
		}, []string{"reason"}),
		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wagering_processing_duration_seconds",
			Help:    "Processing latency of wager transactions.",
			Buckets: prometheus.ExponentialBuckets(0.002, 2, 12),
		}, []string{"source"}),
		OutboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wagering_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event.",
		}),
		OutboxPublishedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_outbox_published_total", Help: "Outbox events published.",
		}, []string{"event_type"}),
		OutboxFailuresTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_outbox_publish_failures_total", Help: "Failed outbox publish attempts.",
		}),
		ReconciliationDivergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_reconciliation_divergences_total", Help: "Reconciliations that found a divergence.",
		}),
		PendingResolutions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_pending_reference_resolutions_total", Help: "Pending reference outcomes.",
		}, []string{"outcome"}),
	}
	reg.MustRegister(m.TransactionsTotal, m.DuplicatesTotal, m.RetriesTotal, m.DLQTotal,
		m.ConcurrencyConflictsTotal, m.ProcessingDuration, m.OutboxLag, m.OutboxPublishedTotal,
		m.OutboxFailuresTotal, m.ReconciliationDivergences, m.PendingResolutions)
	return m
}

// ObserveDuration records the elapsed time since start for source.
func (m *Metrics) ObserveDuration(source string, start time.Time) {
	m.ProcessingDuration.WithLabelValues(source).Observe(time.Since(start).Seconds())
}
