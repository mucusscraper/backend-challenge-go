// Package config carrega e valida a configuração do serviço a partir de
// variáveis de ambiente. A validação ocorre na inicialização (construção do fx),
// de modo que uma instância mal configurada falha rapidamente em vez de servir tráfego.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config é a configuração completa do serviço.
type Config struct {
	// InstanceID identifica este processo nos logs e nos leases do outbox.
	InstanceID string
	LogLevel   string

	HTTP     HTTPConfig
	Postgres PostgresConfig
	AWS      AWSConfig
	SQS      SQSConfig
	OIDC     OIDCConfig
	Outbox   OutboxConfig
	Pending  PendingConfig

	// ShutdownTimeout limita todo o encerramento gracioso.
	ShutdownTimeout time.Duration
}

// HTTPConfig configura o servidor da API.
type HTTPConfig struct {
	Addr              string
	ReadHeaderTimeout time.Duration
	RequestTimeout    time.Duration
}

// PostgresConfig configura o pool de conexões.
type PostgresConfig struct {
	DSN            string
	MaxConns       int32
	ConnectTimeout time.Duration
	// LockTimeout limita quanto tempo uma transação espera por um row lock de carteira.
	LockTimeout time.Duration
}

// AWSConfig configura o SDK da AWS (LocalStack localmente).
type AWSConfig struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
}

// SQSConfig configura o consumidor de entrada e a fila de eventos de saída.
type SQSConfig struct {
	ConsumerEnabled   bool
	ConsumerName      string
	InboundQueue      string
	InboundDLQ        string
	EventsQueue       string
	Pollers           int
	MaxMessages       int32
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	HandlerTimeout    time.Duration
	// RetryBaseDelay/RetryMaxDelay definem o backoff de visibilidade aplicado a
	// falhas transitórias (o redrive do SQS move a mensagem para a DLQ após
	// maxReceiveCount entregas).
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// AllowedProviders é a allow list no nível de domínio de provedores aceitos
	// da fila (a política do broker controla quem pode enviar).
	AllowedProviders []string
}

// OIDCConfig configura a validação de access tokens.
type OIDCConfig struct {
	// Issuer é o claim "iss" esperado.
	Issuer string
	// JWKSURL é onde as chaves de assinatura são buscadas (pode diferir do Issuer
	// quando o IdP é acessado por um hostname interno).
	JWKSURL string
	// Audience é o claim "aud" esperado.
	Audience string
	// ProviderClaim é o claim que contém a identidade do provedor.
	ProviderClaim string
}

// OutboxConfig configura o relay do outbox.
type OutboxConfig struct {
	Enabled      bool
	PollInterval time.Duration
	BatchSize    int
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
}

// PendingConfig configura o worker de referências pendentes.
type PendingConfig struct {
	Enabled      bool
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	TTL          time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
}

// Load lê a configuração do ambiente e a valida.
func Load() (Config, error) {
	return LoadFrom(os.Getenv)
}

// LoadFrom lê a configuração usando getenv (testável).
func LoadFrom(getenv func(string) string) (Config, error) {
	r := reader{getenv: getenv}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: r.str("INSTANCE_ID", host),
		LogLevel:   r.str("LOG_LEVEL", "info"),
		HTTP: HTTPConfig{
			Addr:              r.str("HTTP_ADDR", ":8080"),
			ReadHeaderTimeout: r.dur("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
			RequestTimeout:    r.dur("HTTP_REQUEST_TIMEOUT", 15*time.Second),
		},
		Postgres: PostgresConfig{
			DSN:            r.str("DATABASE_URL", ""),
			MaxConns:       int32(r.int("DATABASE_MAX_CONNS", 20)),
			ConnectTimeout: r.dur("DATABASE_CONNECT_TIMEOUT", 30*time.Second),
			LockTimeout:    r.dur("DATABASE_LOCK_TIMEOUT", 5*time.Second),
		},
		AWS: AWSConfig{
			Region:          r.str("AWS_REGION", "us-east-1"),
			Endpoint:        r.str("AWS_ENDPOINT_URL", ""),
			AccessKeyID:     r.str("AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: r.str("AWS_SECRET_ACCESS_KEY", ""),
		},
		SQS: SQSConfig{
			ConsumerEnabled:   r.bool("SQS_CONSUMER_ENABLED", true),
			ConsumerName:      r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
			InboundQueue:      r.str("SQS_INBOUND_QUEUE", "wager-transactions.fifo"),
			InboundDLQ:        r.str("SQS_INBOUND_DLQ", "wager-transactions-dlq.fifo"),
			EventsQueue:       r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
			Pollers:           r.int("SQS_POLLERS", 2),
			MaxMessages:       int32(r.int("SQS_MAX_MESSAGES", 10)),
			WaitTime:          r.dur("SQS_WAIT_TIME", 10*time.Second),
			VisibilityTimeout: r.dur("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
			HandlerTimeout:    r.dur("SQS_HANDLER_TIMEOUT", 20*time.Second),
			RetryBaseDelay:    r.dur("SQS_RETRY_BASE_DELAY", 2*time.Second),
			RetryMaxDelay:     r.dur("SQS_RETRY_MAX_DELAY", 60*time.Second),
			AllowedProviders:  r.list("SQS_ALLOWED_PROVIDERS", "provider-a,provider-b"),
		},
		OIDC: OIDCConfig{
			Issuer:        r.str("OIDC_ISSUER", ""),
			JWKSURL:       r.str("OIDC_JWKS_URL", ""),
			Audience:      r.str("OIDC_AUDIENCE", "wagering-api"),
			ProviderClaim: r.str("OIDC_PROVIDER_CLAIM", "provider_id"),
		},
		Outbox: OutboxConfig{
			Enabled:      r.bool("OUTBOX_ENABLED", true),
			PollInterval: r.dur("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    r.int("OUTBOX_BATCH_SIZE", 50),
			Lease:        r.dur("OUTBOX_LEASE", 30*time.Second),
			BaseBackoff:  r.dur("OUTBOX_BASE_BACKOFF", time.Second),
			MaxBackoff:   r.dur("OUTBOX_MAX_BACKOFF", 5*time.Minute),
		},
		Pending: PendingConfig{
			Enabled:      r.bool("PENDING_WORKER_ENABLED", true),
			PollInterval: r.dur("PENDING_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    r.int("PENDING_BATCH_SIZE", 50),
			MaxAttempts:  r.int("PENDING_MAX_ATTEMPTS", 10),
			TTL:          r.dur("PENDING_TTL", 10*time.Minute),
			BaseBackoff:  r.dur("PENDING_BASE_BACKOFF", 500*time.Millisecond),
			MaxBackoff:   r.dur("PENDING_MAX_BACKOFF", time.Minute),
		},
		ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
	}
	if c.OIDC.JWKSURL == "" && c.OIDC.Issuer != "" {
		c.OIDC.JWKSURL = strings.TrimRight(c.OIDC.Issuer, "/") + "/protocol/openid-connect/certs"
	}
	if len(r.errs) > 0 {
		return Config{}, errors.Join(r.errs...)
	}
	return c, c.Validate()
}

// Validate verifica valores obrigatórios e timeouts coerentes.
func (c Config) Validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("config: %s is required", name))
		}
	}
	req("DATABASE_URL", c.Postgres.DSN)
	req("OIDC_ISSUER", c.OIDC.Issuer)
	req("OIDC_AUDIENCE", c.OIDC.Audience)
	req("SQS_INBOUND_QUEUE", c.SQS.InboundQueue)
	req("SQS_INBOUND_DLQ", c.SQS.InboundDLQ)
	req("SQS_EVENTS_QUEUE", c.SQS.EventsQueue)
	req("INSTANCE_ID", c.InstanceID)
	if c.SQS.HandlerTimeout >= c.SQS.VisibilityTimeout {
		errs = append(errs, errors.New("config: SQS_HANDLER_TIMEOUT must be lower than SQS_VISIBILITY_TIMEOUT"))
	}
	if c.SQS.MaxMessages < 1 || c.SQS.MaxMessages > 10 {
		errs = append(errs, errors.New("config: SQS_MAX_MESSAGES must be within 1..10"))
	}
	if c.SQS.Pollers < 1 {
		errs = append(errs, errors.New("config: SQS_POLLERS must be >= 1"))
	}
	if c.Pending.MaxAttempts < 1 {
		errs = append(errs, errors.New("config: PENDING_MAX_ATTEMPTS must be >= 1"))
	}
	if c.Outbox.BatchSize < 1 || c.Pending.BatchSize < 1 {
		errs = append(errs, errors.New("config: batch sizes must be >= 1"))
	}
	return errors.Join(errs...)
}

type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) str(key, def string) string {
	if v := r.getenv(key); v != "" {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("config: %s: %w", key, err))
	}
	return n
}

func (r *reader) bool(key string, def bool) bool {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("config: %s: %w", key, err))
	}
	return b
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v := r.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("config: %s: %w", key, err))
	}
	return d
}

func (r *reader) list(key, def string) []string {
	var out []string
	for _, s := range strings.Split(r.str(key, def), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
