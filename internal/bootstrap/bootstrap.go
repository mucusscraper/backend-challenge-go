// Package bootstrap compõe a aplicação com Uber Fx.
//
// Módulos:
//
//	core     validação de config, logger JSON, métricas
//	postgres pool pgx (+ ciclo de vida), unit of work, outbox store
//	sqs      cliente SQS, resolução de filas (+ ciclo de vida), publisher de eventos
//	app      casos de uso (wagering, wallets) e suas políticas
//	auth     verifier de token OIDC
//	http     handlers e servidor
//	workers  consumidor SQS, relay do outbox, loop de referências pendentes
//
// Ordem do ciclo de vida (fx executa OnStart na ordem de registro e OnStop na
// ordem inversa):
//
//	start: postgres pronto -> filas resolvidas -> workers -> servidor HTTP
//	stop:  servidor HTTP (parar recebimento, drenar requisições) -> consumidor SQS (parar
//	       polling, terminar ou liberar mensagens em andamento) -> relay do outbox ->
//	       worker de pendentes -> pool do postgres fechado por último
//
// As dependências são fechadas apenas após todo componente que as usa ter
// terminado.
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

// CoreModule fornece logging e métricas. O Config em si é fornecido pelo
// chamador (config.Load no main, um config de teste nos testes).
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

// PostgresModule fornece o pool e os repositórios.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewUnitOfWork, fx.As(new(app.UnitOfWork))),
		fx.Annotate(postgres.NewOutboxStore, fx.As(new(app.OutboxStore))),
	),
)

// newPool cria o pool e registra seu ciclo de vida: espera até o banco
// responder no início, fecha ao parar.
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

// SQSModule fornece o cliente do broker e o publisher.
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

// AppModule fornece os casos de uso.
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

// AuthModule fornece o verifier de token.
var AuthModule = fx.Module("auth", fx.Provide(auth.NewVerifier))

// HTTPModule fornece os handlers e o servidor.
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

// Workers agrupa os componentes em background.
type Workers struct {
	Consumer *messaging.Consumer
	Outbox   *worker.Loop
	Relay    *worker.OutboxRelay
	Pending  *worker.Loop
}

// WorkersModule fornece os workers em background.
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

// registerLifecycle adiciona os hooks de início/parada dos pontos de entrada em
// uma ordem explícita (veja a documentação do pacote).
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

// Options retorna o grafo completo da aplicação. cfg é fornecido pelo
// chamador; opções extras (ex.: fx.Populate nos testes) são adicionadas.
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
