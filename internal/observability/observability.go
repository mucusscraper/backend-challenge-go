// Package observability fornece logging JSON estruturado (log/slog) e métricas
// Prometheus. Os helpers de log carregam apenas identificadores (correlationId,
// messageId, transactionId, walletId, providerId); credenciais e payloads
// financeiros completos nunca são registrados.
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

// NewLogger constrói um logger JSON gravando no stdout.
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

// WithLogFields retorna um contexto carregando atributos de log extras. Os
// handlers os adicionam automaticamente a todo registro feito com esse contexto.
func WithLogFields(ctx context.Context, args ...any) context.Context {
	prev, _ := ctx.Value(logFieldsKey{}).([]any)
	merged := make([]any, 0, len(prev)+len(args))
	merged = append(merged, prev...)
	merged = append(merged, args...)
	return context.WithValue(ctx, logFieldsKey{}, merged)
}

// contextHandler injeta os atributos armazenados por WithLogFields.
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

// Metrics agrupa todos os coletores Prometheus do serviço.
type Metrics struct {
	Registry *prometheus.Registry

	// TransactionsTotal conta operações processadas por tipo, status final
	// e fonte (http, sqs, worker).
	TransactionsTotal *prometheus.CounterVec
	// DuplicatesTotal conta replays detectados por idempotência (chave de
	// idempotência / id externo) ou pelo inbox (messageId).
	DuplicatesTotal *prometheus.CounterVec
	// RetriesTotal conta retries por componente (db_tx, sqs, outbox, pending).
	RetriesTotal *prometheus.CounterVec
	// DLQTotal conta mensagens enviadas à DLQ por motivo.
	DLQTotal *prometheus.CounterVec
	// ConcurrencyConflictsTotal conta timeouts de lock, corridas de unicidade
	// e incompatibilidades de versão resolvidas por retry.
	ConcurrencyConflictsTotal *prometheus.CounterVec
	// ProcessingDuration mede a latência do caso de uso por fonte.
	ProcessingDuration *prometheus.HistogramVec
	// OutboxLag é a idade em segundos do evento não publicado mais antigo.
	OutboxLag prometheus.Gauge
	// OutboxPublishedTotal conta eventos publicados pelo outbox por tipo.
	OutboxPublishedTotal *prometheus.CounterVec
	// OutboxFailuresTotal conta tentativas de publicação com falha.
	OutboxFailuresTotal prometheus.Counter
	// ReconciliationDivergences conta carteiras cujo saldo armazenado difere
	// do ledger.
	ReconciliationDivergences prometheus.Counter
	// PendingResolutions conta resultados de referências pendentes.
	PendingResolutions *prometheus.CounterVec
}

// NewMetrics registra todos os coletores em um registry dedicado.
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

// ObserveDuration registra o tempo decorrido desde start para a fonte.
func (m *Metrics) ObserveDuration(source string, start time.Time) {
	m.ProcessingDuration.WithLabelValues(source).Observe(time.Since(start).Seconds())
}
