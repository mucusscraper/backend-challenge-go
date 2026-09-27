// Command wallet-service runs the HTTP API, the SQS consumer, the outbox
// relay and the pending-reference worker in one process. Any number of
// instances can run against the same PostgreSQL and SQS.
//
// "wallet-service healthcheck" probes /health/ready of a local instance; it
// is used by the container health check (the image has no shell or curl).
package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/fx"

	"github.com/mucusscraper/backend-challenge-go/internal/bootstrap"
	"github.com/mucusscraper/backend-challenge-go/internal/config"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	// fx.New builds the graph; Run starts it, blocks until SIGINT/SIGTERM
	// and then runs the OnStop hooks within cfg.ShutdownTimeout.
	fx.New(bootstrap.Options(cfg)).Run()
}

func healthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/health/ready")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "not ready:", resp.Status)
		return 1
	}
	return 0
}
