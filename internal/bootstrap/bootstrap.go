// Package bootstrap composes the application with Uber Fx.
//
// Modules:
//
//	core     config validation, JSON logger, metrics
//	postgres pgx pool (+ lifecycle), unit of work, outbox store
//	sqs      SQS client, queue resolution (+ lifecycle), event publisher
//	app      use cases (wagering, wallets) and their policies
//	auth     OIDC token verifier
//	http     handlers and server
//	workers  SQS consumer, outbox relay, pending-reference loop
//
// Lifecycle order (fx runs OnStart in registration order and OnStop in
// reverse):
//
//	start: postgres ready -> queues resolved -> workers -> HTTP server
//	stop:  HTTP server (stop intake, drain requests) -> SQS consumer (stop
//	       polling, finish or release in-flight messages) -> outbox relay ->
//	       pending worker -> postgres pool closed last
//
// Dependencies are therefore closed only after every component using them
// has finished.
package bootstrap

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/auth"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	"github.com/mucusscraper/backend-challenge-go/internal/httpapi"
	"github.com/mucusscraper/backend-challenge-go/internal/messaging"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
	"github.com/mucusscraper/backend-challenge-go/internal/postgres"
	"github.com/mucusscraper/backend-challenge-go/internal/worker"
)

// CoreModule provides logging and metrics. The Config itself is supplied by
// the caller (config.Load in main, a test config in tests).
var CoreModule = fx.Module("core",
	fx.Provide(
		func(cfg config.Config) *slog.Logger { return observability.NewLogger(cfg.LogLevel, cfg.InstanceID) },
		observability.NewMetrics,
		func(cfg config.Config) config.HTTPConfig { return cfg.HTTP },
		func(cfg config.Config) config.PostgresConfig { return cfg.Postgres },
		func(cfg config.Config) config.AWSConfig { return cfg.AWS },
		func(cfg config.Config) config.SQSConfig { return cfg.SQS },
		func(cfg config.Config) config.OIDCConfig { return cfg.OIDC },
		func(cfg config.Config) config.OutboxConfig { return cfg.Outbox },
		func(cfg config.Config) config.PendingConfig { return cfg.Pending },
	),
	fx.Invoke(func(cfg config.Config) error { return cfg.Validate() }),
)

// PostgresModule provides the pool and repositories.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(app.UnitOfWork))),
		fx.Annotate(postgres.NewOutboxStore, fx.As(new(app.OutboxStore))),
	),
)

// newPool creates the pool and registers its lifecycle: wait until the
// database answers on start, close on stop.
func newPool(lc fx.Lifecycle, cfg config.PostgresConfig, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(cfg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return postgres.WaitReady(ctx, pool, cfg.ConnectTimeout, log)
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

// SQSModule provides the broker client and publisher.
var SQSModule = fx.Module("sqs",
	fx.Provide(
		fx.Annotate(func(cfg config.AWSConfig) (messaging.API, error) {
			return messaging.NewClient(cfg)
		}),
		newQueues,
		fx.Annotate(messaging.NewPublisher, fx.As(new(app.EventPublisher))),
	),
)

func newQueues(lc fx.Lifecycle, api messaging.API, cfg config.Config, log *slog.Logger) *messaging.Queues {
	q := messaging.NewQueues(api, cfg.SQS)
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		return q.Resolve(ctx, cfg.Postgres.ConnectTimeout, log)
	}})
	return q
}

// AppModule provides the use cases.
var AppModule = fx.Module("app",
	fx.Provide(
		func() app.Clock { return app.SystemClock },
		func() domain.IDGenerator { return app.NewUUIDv7 },
		func(cfg config.PendingConfig) domain.ReferencePolicy {
			return domain.ReferencePolicy{
				MaxAttempts: cfg.MaxAttempts, TTL: cfg.TTL, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff,
			}
		},
		app.NewWageringService,
		app.NewWalletService,
	),
)

// AuthModule provides the token verifier.
var AuthModule = fx.Module("auth", fx.Provide(auth.NewVerifier))

// HTTPModule provides the handlers and server.
var HTTPModule = fx.Module("http",
	fx.Provide(
		httpapi.NewHandlers,
		func(pool *pgxpool.Pool, q *messaging.Queues) []httpapi.ReadinessCheck {
			return []httpapi.ReadinessCheck{
				{Name: "postgres", Check: pool.Ping},
				{Name: "sqs", Check: q.Ping},
			}
		},
		httpapi.NewServer,
	),
)

// Workers groups the background components.
type Workers struct {
	Consumer *messaging.Consumer
	Outbox   *worker.Loop
	Relay    *worker.OutboxRelay
	Pending  *worker.Loop
}

// WorkersModule provides the background workers.
var WorkersModule = fx.Module("workers",
	fx.Provide(
		messaging.NewConsumer,
		func(store app.OutboxStore, pub app.EventPublisher, cfg config.Config, log *slog.Logger,
			m *observability.Metrics) *worker.OutboxRelay {
			return worker.NewOutboxRelay(store, pub, cfg.Outbox, cfg.InstanceID, log, m)
		},
		func(c *messaging.Consumer, r *worker.OutboxRelay, svc *app.WageringService, cfg config.Config,
			log *slog.Logger) *Workers {
			return &Workers{
				Consumer: c,
				Relay:    r,
				Outbox:   worker.NewOutboxLoop(r, cfg.Outbox, log),
				Pending:  worker.NewPendingLoop(svc, cfg.Pending, log),
			}
		},
	),
)

// registerLifecycle appends the start/stop hooks of the entry points in an
// explicit order (see package documentation).
func registerLifecycle(lc fx.Lifecycle, cfg config.Config, w *Workers, srv *httpapi.Server,
	_ *pgxpool.Pool, _ *messaging.Queues) {
	if cfg.Pending.Enabled {
		lc.Append(fx.Hook{OnStart: w.Pending.Start, OnStop: w.Pending.Stop})
	}
	if cfg.Outbox.Enabled {
		lc.Append(fx.Hook{OnStart: w.Outbox.Start, OnStop: w.Outbox.Stop})
	}
	if cfg.SQS.ConsumerEnabled {
		lc.Append(fx.Hook{OnStart: w.Consumer.Start, OnStop: w.Consumer.Stop})
	}
	lc.Append(fx.Hook{OnStart: srv.Start, OnStop: srv.Stop})
}

// Options returns the whole application graph. cfg is supplied by the
// caller; extra options (e.g. fx.Populate in tests) are appended.
func Options(cfg config.Config, extra ...fx.Option) fx.Option {
	opts := []fx.Option{
		fx.Supply(cfg),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		fx.StartTimeout(cfg.Postgres.ConnectTimeout + 10*time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout),
		CoreModule,
		PostgresModule,
		SQSModule,
		AppModule,
		AuthModule,
		HTTPModule,
		WorkersModule,
		fx.Invoke(registerLifecycle),
	}
	return fx.Options(append(opts, extra...)...)
}
