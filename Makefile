# Convenience targets. Every command is also documented in README.md.
OWNER_DSN ?= postgres://wagering_owner:wagering_owner@localhost:55432/wagering?sslmode=disable

.PHONY: up infra down clean fmt vet test test-race test-integration test-e2e test-chaos test-all \
        migrate-up migrate-down migrate-status token-provider-a token-wallet-service

up: ## full stack: infra + migrations + 3 service instances
	docker compose up --build -d

infra: ## only PostgreSQL, Keycloak and LocalStack (for integration tests)
	docker compose up -d postgres keycloak localstack

down:
	docker compose down

clean: ## also removes the database volume
	docker compose down -v

fmt:
	gofmt -w .

vet:
	go vet ./...
	go vet -tags integration ./...
	go vet -tags e2e ./...

test: ## unit tests, no infrastructure needed
	go test ./...

test-race:
	go test -race ./...

test-integration: infra ## real PostgreSQL/Keycloak/LocalStack, in-process instances
	go test -race -count=1 -tags integration ./test/integration/...

test-e2e: up ## three service processes from docker compose
	go test -race -count=1 -tags e2e ./test/e2e/...

test-chaos: up ## kills app1 with SIGKILL during load
	E2E_CHAOS=1 go test -count=1 -tags e2e -run Chaos ./test/e2e/...

test-all: test-race test-integration test-e2e

migrate-up:
	MIGRATION_DATABASE_URL="$(OWNER_DSN)" go run ./cmd/migrate up

migrate-down: ## reverts the latest migration
	MIGRATION_DATABASE_URL="$(OWNER_DSN)" go run ./cmd/migrate down

migrate-status:
	MIGRATION_DATABASE_URL="$(OWNER_DSN)" go run ./cmd/migrate status

token-provider-a:
	@curl -s -X POST http://localhost:8080/realms/wagering/protocol/openid-connect/token \
	  -d grant_type=client_credentials -d client_id=provider-a -d client_secret=provider-a-secret | jq -r .access_token

token-wallet-service:
	@curl -s -X POST http://localhost:8080/realms/wagering/protocol/openid-connect/token \
	  -d grant_type=client_credentials -d client_id=wallet-service -d client_secret=wallet-service-secret | jq -r .access_token
