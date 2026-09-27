package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mucusscraper/backend-challenge-go/internal/auth"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
	"github.com/mucusscraper/backend-challenge-go/internal/observability"
)

// ReadinessCheck is a named dependency probe used by /health/ready.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// Server is the HTTP server with fx-managed lifecycle.
type Server struct {
	srv      *http.Server
	cfg      config.HTTPConfig
	log      *slog.Logger
	draining atomic.Bool
	listener net.Listener
	done     chan struct{}
}

// NewServer builds the router and the server.
func NewServer(cfg config.HTTPConfig, h *Handlers, verifier *auth.Verifier, metrics *observability.Metrics,
	checks []ReadinessCheck, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, log: log}
	mux := http.NewServeMux()

	// Public endpoints.
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	})
	mux.HandleFunc("GET /health/ready", s.ready(checks))
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))

	// Internal wallet service.
	operator := requireRole(verifier, func(p auth.Principal) bool { return p.IsWalletOperator() })
	mux.Handle("POST /wallets", operator(http.HandlerFunc(h.openWallet)))
	mux.Handle("GET /wallets/{walletId}", operator(http.HandlerFunc(h.getWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", operator(http.HandlerFunc(h.getLedger)))
	mux.Handle("POST /wallets/{walletId}/reconciliation", operator(http.HandlerFunc(h.reconcile)))

	// Providers.
	provider := requireRole(verifier, func(p auth.Principal) bool { return p.IsProvider() })
	anyone := requireRole(verifier, func(p auth.Principal) bool { return p.IsProvider() || p.IsWalletOperator() })
	mux.Handle("POST /wagering/transactions", provider(http.HandlerFunc(h.submitTransaction)))
	mux.Handle("GET /wagering/transactions/{transactionId}", anyone(http.HandlerFunc(h.getTransaction)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		anyone(http.HandlerFunc(h.getProviderTransaction)))

	handler := withCorrelation(withAccessLog(log, withTimeout(cfg.RequestTimeout, mux)))
	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.RequestTimeout,
		WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

// Handler exposes the root handler (tests).
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// Addr returns the bound address once started.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Start binds the port synchronously (fails fast if unavailable) and
// serves in the background.
func (s *Server) Start(context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server failed", "error", err)
		}
	}()
	s.log.Info("http server started", "addr", ln.Addr().String())
	return nil
}

// Stop reports not-ready, stops accepting connections and waits for
// in-flight requests (bounded by ctx).
func (s *Server) Stop(ctx context.Context) error {
	s.draining.Store(true)
	err := s.srv.Shutdown(ctx)
	if s.done != nil {
		<-s.done
	}
	s.log.Info("http server stopped")
	return err
}

func (s *Server) ready(checks []ReadinessCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := map[string]string{}
		status := http.StatusOK
		if s.draining.Load() {
			status = http.StatusServiceUnavailable
			result["server"] = "DRAINING"
		}
		for _, c := range checks {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := c.Check(ctx)
			cancel()
			if err != nil {
				status = http.StatusServiceUnavailable
				result[c.Name] = "DOWN"
				s.log.WarnContext(r.Context(), "readiness check failed", "dependency", c.Name, "error", err)
			} else {
				result[c.Name] = "UP"
			}
		}
		overall := "UP"
		if status != http.StatusOK {
			overall = "DOWN"
		}
		writeJSON(w, status, map[string]any{"status": overall, "checks": result})
	}
}

// --- middleware ----------------------------------------------------------------

type correlationKey struct{}

func correlationID(ctx context.Context) string {
	v, _ := ctx.Value(correlationKey{}).(string)
	return v
}

// withCorrelation propagates X-Correlation-Id (or generates one) into the
// context, the logs and the response.
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if !validCorrelationID(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", id)
		ctx := context.WithValue(r.Context(), correlationKey{}, id)
		ctx = observability.WithLogFields(ctx, "correlationId", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func validCorrelationID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !unicode.IsPrint(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func withTimeout(d time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withAccessLog logs one line per request with identifiers only (no body,
// no Authorization header).
func withAccessLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/health/live" || r.URL.Path == "/health/ready" || r.URL.Path == "/metrics" {
			return
		}
		log.InfoContext(r.Context(), "http request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "durationMs", time.Since(start).Milliseconds())
	})
}

// requireRole authenticates the bearer token and applies allow. Missing or
// invalid tokens get 401; valid tokens without permission get 403. No
// handler (hence no financial effect) runs in either case.
func requireRole(v *auth.Verifier, allow func(auth.Principal) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, err := v.Authenticate(r.Context(), r.Header.Get("Authorization"))
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
				writeError(w, r, http.StatusUnauthorized, codeUnauthenticated, err.Error())
				return
			}
			if !allow(p) {
				writeError(w, r, http.StatusForbidden, codeForbidden, "insufficient permissions")
				return
			}
			ctx := auth.WithPrincipal(r.Context(), p)
			if p.ProviderID != "" {
				ctx = observability.WithLogFields(ctx, "providerId", p.ProviderID)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
