# Wager Wallet Service — distributed bet processing in Go

A Go service (Uber Fx, pgx, PostgreSQL, SQS/LocalStack, Keycloak) that moves
player wallets for game-provider operations (`BET`, `WIN`, `LOSS`, `REFUND`,
`ROLLBACK`) through an **HTTP API** and an **SQS consumer**. Both entry points share the same
use case and the same guarantees. The financial result stays correct with
several instances running and with crashes between processing steps.

- Challenge statement: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Design decisions, guarantees, limitations: [ARCHITECTURE.md](ARCHITECTURE.md)

```
             ┌──────────┐  client_credentials   ┌──────────┐
 providers ─▶│ Keycloak │◀──────────────────────│ internal │
             └────┬─────┘  JWT (JWKS)           │ service  │
                  │                              └────┬─────┘
   HTTP (Bearer)  ▼                                   │ HTTP
 ┌──────────────────────────────────────────────────────────────┐
 │  app1 / app2 / app3  (independent processes, same binary)    │
 │  HTTP API ─┐                                                 │
 │  SQS consumer ─┼─▶ WageringService / WalletService ─▶ PostgreSQL │
 │  pending-reference worker ┘      (one SQL tx per operation:  │
 │  outbox relay ──────────▶ SQS     wallet+tx+ledger+inbox+outbox)
 └──────────────────────────────────────────────────────────────┘
   wager-transactions.fifo ──▶ consumer      (DLQ: wager-transactions-dlq.fifo)
   outbox relay ──▶ wallet-events.fifo
```

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Quick start](#2-quick-start)
3. [Environment variables](#3-environment-variables)
4. [Queues](#4-queues)
5. [Migrations](#5-migrations)
6. [Running the application](#6-running-the-application)
7. [Authentication and test identities](#7-authentication-and-test-identities)
8. [HTTP API](#8-http-api)
9. [Sending operations through SQS](#9-sending-operations-through-sqs)
10. [Tests](#10-tests)
11. [Observability](#11-observability)
12. [Project layout](#12-project-layout)

## 1. Prerequisites

| Tool | Version used | Needed for |
| --- | --- | --- |
| Docker + Docker Compose v2 | Docker 29, Compose 5 | the whole environment |
| Go | **1.25** (see `go.mod` and `Dockerfile`) | running tests / tools locally |
| `curl`, `jq` | any | examples below |

Host ports used: `8080` (Keycloak), `8081–8083` (service instances), `4566`
(LocalStack), `55432` (PostgreSQL; not 5432, to avoid clashing with a local
PostgreSQL).

## 2. Quick start

```sh
docker compose up --build        # add -d to detach
```

This starts, in order:

1. `postgres` (creates the restricted runtime role `wallet_app`),
2. `keycloak` (imports the `wagering` realm with test clients),
3. `localstack` (creates the SQS queues, redrive policy and queue policies),
4. `migrate`, a one-shot job that applies the migrations and exits with 0,
5. `app1`, `app2`, `app3`, three independent instances of the service.

When every instance is `healthy` (`docker compose ps`):

```sh
curl -s localhost:8081/health/ready   # {"checks":{"postgres":"UP","sqs":"UP"},"status":"UP"}
```

Stop with `docker compose down`, or `docker compose down -v` to also drop the database.

## 3. Environment variables

`docker-compose.yml` has a default for every variable. To override any of
them, copy [.env.example](.env.example) to `.env`. The example contains local
values only, no real secrets.

Main variables read by the service binary:

| Variable | Default | Meaning |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | instance name in logs and outbox leases |
| `HTTP_ADDR` | `:8080` | listen address |
| `DATABASE_URL` | — (required) | runtime DSN (`wallet_app` role) |
| `DATABASE_LOCK_TIMEOUT` | `5s` | max wait for a wallet row lock |
| `MIGRATION_DATABASE_URL` | — | owner DSN used by `migrate` |
| `AWS_ENDPOINT_URL` | — | LocalStack endpoint (`http://localstack:4566`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | broker credentials of the service |
| `SQS_INBOUND_QUEUE` / `SQS_INBOUND_DLQ` / `SQS_EVENTS_QUEUE` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | queue names |
| `SQS_VISIBILITY_TIMEOUT` / `SQS_HANDLER_TIMEOUT` | `30s` / `20s` | handler timeout must be lower than the visibility timeout |
| `SQS_RETRY_BASE_DELAY` / `SQS_RETRY_MAX_DELAY` | `2s` / `60s` | backoff applied to transient failures |
| `SQS_ALLOWED_PROVIDERS` | `provider-a,provider-b` | providers accepted from the queue |
| `OIDC_ISSUER` | — (required) | expected `iss` (`http://localhost:8080/realms/wagering`) |
| `OIDC_JWKS_URL` | issuer + `/protocol/openid-connect/certs` | where signing keys are fetched |
| `OIDC_AUDIENCE` | `wagering-api` | expected `aud` |
| `PENDING_MAX_ATTEMPTS` / `PENDING_TTL` | `10` / `10m` | budget of a pending reference |
| `OUTBOX_LEASE` | `30s` | lease before abandoned outbox rows are reclaimed |
| `SHUTDOWN_TIMEOUT` | `25s` | graceful shutdown budget |

[.env.example](.env.example) lists all of them.

## 4. Queues

The queues are created automatically by
[deploy/localstack/init-sqs.sh](deploy/localstack/init-sqs.sh), a LocalStack
"ready" hook:

| Queue | Type | Purpose |
| --- | --- | --- |
| `wager-transactions.fifo` | FIFO, visibility 30s, **redrive to DLQ after 5 receives** | inbound operations |
| `wager-transactions-dlq.fifo` | FIFO, 14-day retention | dead letters (redrive + permanent errors) |
| `wallet-events.fifo` | FIFO | outbound integration events (outbox) |

To re-run the provisioning manually (the script is idempotent):

```sh
docker compose exec localstack /etc/localstack/init/ready.d/init-sqs.sh
docker compose exec localstack awslocal sqs list-queues
```

## 5. Migrations

Migrations are versioned SQL files in [migrations/](migrations), in goose
format with `Up` and `Down` sections. They are embedded in the binaries and
run by `cmd/migrate` with the owner role.

```sh
# with docker compose (runs automatically on `up`):
docker compose run --rm migrate                                         # apply all
docker compose run --rm --entrypoint /app/migrate migrate status
docker compose run --rm --entrypoint /app/migrate migrate down          # revert the latest
docker compose run --rm --entrypoint /app/migrate migrate down-to 0     # revert everything

# from the host (Go installed):
export MIGRATION_DATABASE_URL='postgres://wagering_owner:wagering_owner@localhost:55432/wagering?sslmode=disable'
go run ./cmd/migrate up | down | down-to N | status
# or: make migrate-up / make migrate-down / make migrate-status
```

| Version | Content |
| --- | --- |
| 00001 | wallets, wager transactions, ledger, all constraints and protection triggers |
| 00002 | inbox, outbox, their triggers, and grants for the runtime role |

## 6. Running the application

Docker Compose already runs three instances. To run one instance from source
against the Compose infrastructure:

```sh
docker compose up -d postgres keycloak localstack migrate
DATABASE_URL='postgres://wallet_app:wallet_app@localhost:55432/wagering?sslmode=disable' \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=wallet-service AWS_SECRET_ACCESS_KEY=x \
OIDC_ISSUER=http://localhost:8080/realms/wagering HTTP_ADDR=:8090 INSTANCE_ID=local \
go run ./cmd/wallet-service
```

`SIGTERM`/`SIGINT` trigger a graceful shutdown (see ARCHITECTURE.md §13).

## 7. Authentication and test identities

Keycloak is provisioned from
[deploy/keycloak/realm-wagering.json](deploy/keycloak/realm-wagering.json)
with confidential clients using the **client_credentials** grant:

| Client ID | Secret | Role | `provider_id` claim | Use |
| --- | --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | game provider A |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | game provider B |
| `wallet-service` | `wallet-service-secret` | `wallet-operator` | — | internal service (wallet operations) |
| `provider-a-shortlived` | `provider-a-shortlived-secret` | `wagering-provider` | `provider-a` | 2-second tokens (expiry tests) |
| `no-role-client` | `no-role-client-secret` | — | — | authenticated but without permissions |

Keycloak admin console: <http://localhost:8080> (`admin` / `admin`).

```sh
token() {
  curl -s -X POST http://localhost:8080/realms/wagering/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" | jq -r .access_token
}
OP=$(token wallet-service wallet-service-secret)   # internal service
PA=$(token provider-a provider-a-secret)           # provider A
PB=$(token provider-b provider-b-secret)           # provider B
```

Permissions:

| Endpoint | `wallet-operator` | `wagering-provider` |
| --- | --- | --- |
| `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` | ✅ | ❌ 403 |
| `POST /wagering/transactions` | ❌ 403 | ✅ only with `providerId` = own `provider_id` |
| `GET /wagering/transactions/{id}` | ✅ any | ✅ own transactions only (others: 404) |
| `GET /providers/{providerId}/wagering/transactions/{ext}` | ✅ any | ✅ only own `providerId` (others: 403) |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | public | public |

## 8. HTTP API

The examples use `app1` (`:8081`); any instance gives the same results.

### Open a wallet

```sh
curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```
```json
HTTP 201
{"id":"01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
 "balance":{"amount":"1000.00","currency":"BRL"},"version":1,
 "createdAt":"2026-09-25T19:33:33.137Z","updatedAt":"2026-09-25T19:33:33.137Z"}
```
A second wallet for the same player and currency returns `409 WALLET_ALREADY_EXISTS`.

### Submit an operation

```sh
WALLET=01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052
curl -s -X POST localhost:8082/wagering/transactions -H "Authorization: Bearer $PA" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123",
       "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
```
```json
HTTP 200
{"transactionId":"01a0da0f-077d-7c70-8275-05e84ea2d57b","status":"PROCESSED",
 "balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```
Sending it again, to any instance, returns the same body with
`"idempotentReplay": true`. The balance is the one observed when the operation
was first processed, even if the wallet has moved since.

For `REFUND`/`ROLLBACK`, add `"referenceExternalTransactionId": "..."` to the body.

### Response contract

| Situation | HTTP | Body |
| --- | --- | --- |
| Processed (new or replay) | `200` | `{transactionId, status:"PROCESSED", balance, idempotentReplay}` |
| Waiting for a reference / pending | `202` | `{transactionId, status:"PENDING_REFERENCE", nextAttemptAt, idempotentReplay}` |
| Business rejection (persisted, terminal) | `422` | `{transactionId, status:"REJECTED", failureCode, balance, idempotentReplay}` |
| Invalid input (not persisted) | `400` | `{"error":{"code":"VALIDATION_ERROR" \| "INVALID_JSON" \| "MISSING_IDEMPOTENCY_KEY", "message"}, correlationId}` |
| Missing / invalid / expired token | `401` | `{"error":{"code":"UNAUTHENTICATED"}}` |
| Not allowed | `403` | `{"error":{"code":"FORBIDDEN"}}` |
| Unknown wallet or transaction | `404` | `{"error":{"code":"WALLET_NOT_FOUND" \| "NOT_FOUND"}}` |
| Key reused with another payload | `409` | `{"error":{"code":"IDEMPOTENCY_KEY_CONFLICT"}}` |
| External id reused with another key | `409` | `{"error":{"code":"EXTERNAL_TRANSACTION_CONFLICT"}}` |
| Transient unavailability (DB down, lock timeout) | `503` + `Retry-After: 1` | `{"error":{"code":"SERVICE_UNAVAILABLE"}}` (retry with the same key) |

Examples:

```json
HTTP 422
{"transactionId":"01a0da0f-07b1-7881-a13b-1b9c9309de62","status":"REJECTED",
 "balance":{"amount":"975.00","currency":"BRL"},"failureCode":"INSUFFICIENT_FUNDS","idempotentReplay":false}

HTTP 202
{"transactionId":"01a0da0f-07c3-7a71-b981-2bf116b572cf","status":"PENDING_REFERENCE",
 "nextAttemptAt":"2026-09-25T19:33:33.751Z","idempotentReplay":false}

HTTP 409
{"error":{"code":"IDEMPOTENCY_KEY_CONFLICT","message":"idempotency key reused with a different payload"},
 "correlationId":"9a082148-1b37-4d4c-b803-4db6ab82723c"}
```

### Failure codes

The codes are stable. **Input errors** are never persisted: fix the request and
resend it with the same key. **Rejections** are persisted and final for that
`(providerId, externalTransactionId)`: a corrected operation needs a new
external id.

| `failureCode` | Meaning | Nature |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | a `BET` larger than the balance | definitive outcome |
| `REVERSAL_INSUFFICIENT_FUNDS` | a `ROLLBACK` that must debit (it reverses a `WIN`/`REFUND`) larger than the balance | definitive outcome |
| `REFERENCE_NOT_FOUND` | the reference never arrived within the retry budget/TTL | definitive outcome |
| `REFERENCE_NOT_PROCESSED` | the reference ended `REJECTED`/`FAILED` (or was still pending at expiry) | definitive outcome |
| `REFERENCE_ALREADY_REVERSED` | the reference already has a successful `REFUND`/`ROLLBACK` | definitive outcome |
| `CURRENCY_MISMATCH` | operation currency ≠ wallet currency | client data error (use a new external id) |
| `PLAYER_WALLET_MISMATCH` | the wallet does not belong to the player | client data error |
| `REFERENCE_MISMATCH` | provider/player/wallet/currency/round differ from the reference | client data error |
| `REFERENCE_KIND_NOT_ALLOWED` | e.g. `REFUND` of a `WIN`, `ROLLBACK` of a `ROLLBACK` | client data error |
| `REVERSAL_AMOUNT_MISMATCH` | partial reversal (amount ≠ referenced amount) | client data error |
| `PROCESSING_FAILED` | permanent infrastructure failure while resuming (status `FAILED`) | audit |

### Queries

```sh
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $OP"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $OP"          # → {entries, nextCursor}
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50&cursor=djE6NDY0" -H "Authorization: Bearer $OP"
curl -s localhost:8081/wagering/transactions/<transactionId> -H "Authorization: Bearer $PA"
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PA"
```

Transaction queries return the status, `failureCode`, `attempts`,
`nextAttemptAt`, `referenceExpiresAt`, the resolved `referenceTransactionId`
and the result `balance`, so pending operations can be followed.

### Reconciliation

```sh
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $OP"
```
```json
{"walletId":"01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052",
 "storedBalance":{"amount":"975.00","currency":"BRL"},"calculatedBalance":{"amount":"975.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

## 9. Sending operations through SQS

```sh
WALLET=01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052
BODY='{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
 "data":{"providerId":"provider-a","externalTransactionId":"transaction-456","idempotencyKey":"provider-a:transaction-456",
 "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987",
 "gameId":"fortune-chimp","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}'
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 --message-body "$BODY"

# inspect the outcome / DLQ / published events
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-456 -H "Authorization: Bearer $PA"
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo --message-attribute-names All
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo
```

Routing contract: `MessageGroupId = walletId`, `MessageDeduplicationId = messageId`
(ARCHITECTURE.md §10).

## 10. Tests

| Suite | Needs | Command |
| --- | --- | --- |
| Unit (domain, money, config, parsing…) | nothing | `go test ./...` |
| Unit with race detector | nothing | `go test -race ./...` |
| Static checks | nothing | `go vet ./...` (plus `-tags integration`, `-tags e2e`) |
| Integration: real PostgreSQL, Keycloak and LocalStack; in-process instances | `docker compose up -d postgres keycloak localstack` | `go test -race -count=1 -tags integration ./test/integration/...` |
| End-to-end: three service processes | `docker compose up -d --build` | `go test -race -count=1 -tags e2e ./test/e2e/...` |
| Failure simulation: SIGKILL of `app1` under load | full stack | `E2E_CHAOS=1 go test -count=1 -tags e2e -run Chaos ./test/e2e/...` |

Shortcuts: `make test`, `make test-race`, `make test-integration`,
`make test-e2e`, `make test-chaos`, `make vet`.

The integration suite applies the migrations itself. It uses the restricted
`wallet_app` role, as production does, and creates **its own SQS queues per
test**, so it can run while the Compose instances are up. The e2e suite needs
the three Compose instances healthy.

What the suites cover, mapped to challenge §13:

| Requirement | Test |
| --- | --- |
| Money parsing, scale, limits, overflow, invalid input, currency mismatch | `internal/domain/money/money_test.go` (+ fuzz `FuzzParse`) |
| Wallet invariants, state transitions, 5 kinds, zero policy, opening + events, payload conflict | `internal/domain/domain_test.go` |
| Migrations up/down, constraints, ledger immutability, atomicity | `test/integration/db_test.go` |
| Same bet ×50 in parallel → one debit | `TestSameBetFiftyTimesInParallel`, `TestE2ESameBetFiftyTimesAcrossInstances` |
| 100.00 vs two concurrent 80.00 bets | `TestTwoConcurrentBetsOnLimitedBalance`, `TestE2ETwoBetsRaceAcrossInstances` (10 rounds each) |
| Independent wallets in parallel / no global lock | `TestIndependentWalletsProgressInParallel`, `TestE2EDistinctWalletsInParallel` |
| ≥ 3 independent instances | e2e suite (`app1..app3` containers) + 3 pools in integration |
| Consumer killed after commit, before delete → redelivery | `TestCrashAfterCommitBeforeDelete` |
| Retries, backoff, DLQ, invalid messages | `TestTransientFailuresExhaustToDLQ`, `TestInvalidMessagesGoToDLQ` |
| Two competing publishers + recovery between publish and confirmation | `TestOutboxCompetingPublishersAndRecovery`, `TestE2EOutboxPublishes` |
| Reversal before its reference: resolution / expiry | `TestReversalBeforeReferenceIsResolvedLater`, `TestPendingReferenceExpires`, `TestE2EReversalBeforeReferenceAcrossInstances` |
| Restart preserves idempotency, pending work and consistency | `TestRestartPreservesPendingAndIdempotency`, `TestResumeCrashedPending`, `TestIdempotencyRules`, `TestE2EChaosKillInstance` |
| Same operation via HTTP and SQS | `TestSameOperationThroughHTTPAndSQS`, `TestE2EHTTPAndSQSSameOperation` |
| Fx start/stop, worker termination, resource release | `TestFxLifecycle` |
| Real IdP; missing/invalid/expired credentials; provider isolation; no financial effect | `TestAuthentication`, `TestAuthorizationAndProviderIsolation` |
| Stored balance = credits − debits at the end | `AssertConsistent` / `reconcile` in the tests |

## 11. Observability

- **Logs**: JSON (`log/slog`) on stdout. Each line carries the identifiers that
  are available: `instance`, `correlationId` (`X-Correlation-Id` header, or the
  SQS `messageId`), `messageId`, `transactionId`, `walletId`, `providerId`.
  Tokens and full financial payloads are never logged.
- **Metrics**: Prometheus at `GET /metrics`.

| Metric | Labels | What |
| --- | --- | --- |
| `wagering_transactions_total` | kind, status, source | results by status |
| `wagering_duplicates_total` | source, mechanism (`idempotency`/`inbox`) | duplicates detected |
| `wagering_retries_total` | component | retries (db_tx, sqs, sqs_receive, outbox, pending) |
| `wagering_sqs_dlq_total` | reason | messages sent to the DLQ |
| `wagering_concurrency_conflicts_total` | reason | races resolved by retry |
| `wagering_outbox_lag_seconds` | — | age of the oldest unpublished event |
| `wagering_outbox_published_total` / `wagering_outbox_publish_failures_total` | event_type | outbox delivery |
| `wagering_processing_duration_seconds` | source | processing latency histogram |
| `wagering_reconciliation_divergences_total` | — | reconciliation divergences |
| `wagering_pending_reference_resolutions_total` | outcome | pending-reference outcomes |

- **Health**: `GET /health/live` (process) and `GET /health/ready` (PostgreSQL
  ping + SQS `GetQueueAttributes`; returns 503 while draining on shutdown).

## 12. Project layout

```
cmd/wallet-service     service entry point (fx.New(...).Run())
cmd/migrate            migration CLI (up, down, down-to, status)
internal/domain        entities, state machine, business rules, events (no infra imports)
internal/domain/money  Money value object (int64 minor units)
internal/app           use cases + ports (UnitOfWork, repositories)
internal/postgres      pgx adapter: SQL, transactions, locks, error classification, migrator
internal/messaging     SQS client, inbound consumer, event publisher
internal/worker        lifecycle-managed loops, outbox relay
internal/auth          OIDC access-token validation, principal/roles
internal/httpapi       net/http handlers, middleware, server lifecycle
internal/observability slog JSON logger, Prometheus metrics
internal/bootstrap     Fx modules and lifecycle ordering
migrations             versioned SQL (goose), embedded
deploy                 Keycloak realm, LocalStack init, PostgreSQL role init
test/integration       integration suite (build tag integration)
test/e2e               multi-process suite (build tag e2e)
```
