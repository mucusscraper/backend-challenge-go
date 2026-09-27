// Command wallet-service executa a API HTTP, o consumer SQS, o outbox
// relay e o worker de referências pendentes em um único processo. Qualquer número de
// instâncias pode rodar contra o mesmo PostgreSQL e SQS.
//
// "wallet-service healthcheck" sonda /health/ready de uma instância local; é
// usado pelo health check do container (a imagem não tem shell nem curl).
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
	// fx.New constrói o grafo; Run o inicia, bloqueia até SIGINT/SIGTERM
	// e então executa os hooks OnStop dentro de cfg.ShutdownTimeout.
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
