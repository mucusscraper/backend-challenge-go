# Wager Wallet Service — processamento distribuído de apostas em Go

Um serviço Go (Uber Fx, pgx, PostgreSQL, SQS/LocalStack, Keycloak) que movimenta
carteiras de jogadores para operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`,
`ROLLBACK`) por meio de uma **API HTTP** e um **consumidor SQS**. Ambos os pontos de entrada
compartilham o mesmo caso de uso e as mesmas garantias. O resultado financeiro permanece
correto com várias instâncias em execução e com falhas entre as etapas de processamento.

- Enunciado do desafio: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Decisões de design, garantias, limitações: [ARCHITECTURE.md](ARCHITECTURE.md)

```
             ┌──────────┐  client_credentials   ┌──────────┐
 providers ─▶│ Keycloak │◀──────────────────────│ internal │
             └────┬─────┘  JWT (JWKS)           │ service  │
                  │                              └────┬─────┘
   HTTP (Bearer)  ▼                                   │ HTTP
 ┌──────────────────────────────────────────────────────────────┐
 │  app1 / app2 / app3  (processos independentes, mesmo binário)│
 │  HTTP API ─┐                                                 │
 │  SQS consumer ─┼─▶ WageringService / WalletService ─▶ PostgreSQL │
 │  pending-reference worker ┘      (uma TX SQL por operação:   │
 │  outbox relay ──────────▶ SQS     wallet+tx+ledger+inbox+outbox)
 └──────────────────────────────────────────────────────────────┘
   wager-transactions.fifo ──▶ consumer      (DLQ: wager-transactions-dlq.fifo)
   outbox relay ──▶ wallet-events.fifo
```

## Índice

1. [Pré-requisitos](#1-pré-requisitos)
2. [Início rápido](#2-início-rápido)
3. [Variáveis de ambiente](#3-variáveis-de-ambiente)
4. [Filas](#4-filas)
5. [Migrações](#5-migrações)
6. [Executando a aplicação](#6-executando-a-aplicação)
7. [Autenticação e identidades de teste](#7-autenticação-e-identidades-de-teste)
8. [API HTTP](#8-api-http)
9. [Enviando operações pelo SQS](#9-enviando-operações-pelo-sqs)
10. [Testes](#10-testes)
11. [Observabilidade](#11-observabilidade)
12. [Estrutura do projeto](#12-estrutura-do-projeto)

## 1. Pré-requisitos

| Ferramenta | Versão utilizada | Necessária para |
| --- | --- | --- |
| Docker + Docker Compose v2 | Docker 29, Compose 5 | todo o ambiente |
| Go | **1.25** (veja `go.mod` e `Dockerfile`) | executar testes / ferramentas localmente |
| `curl`, `jq` | qualquer | exemplos abaixo |

Portas do host utilizadas: `8080` (Keycloak), `8081–8083` (instâncias do serviço), `4566`
(LocalStack), `55432` (PostgreSQL; não 5432, para evitar conflito com um PostgreSQL local).

## 2. Início rápido

```sh
docker compose up --build        # adicione -d para desanexar
```

Isso inicia, na ordem:

1. `postgres` (cria o papel restrito de runtime `wallet_app`),
2. `keycloak` (importa o realm `wagering` com clientes de teste),
3. `localstack` (cria as filas SQS, política de redrive e permissões),
4. `migrate`, uma tarefa única que aplica as migrações e encerra com 0,
5. `app1`, `app2`, `app3`, três instâncias independentes do serviço.

Quando todas as instâncias estiverem `healthy` (`docker compose ps`):

```sh
curl -s localhost:8081/health/ready   # {"checks":{"postgres":"UP","sqs":"UP"},"status":"UP"}
```

Pare com `docker compose down`, ou `docker compose down -v` para também remover o banco de dados.

## 3. Variáveis de ambiente

`docker-compose.yml` possui um valor padrão para cada variável. Para sobrescrever qualquer uma
delas, copie [.env.example](.env.example) para `.env`. O exemplo contém apenas valores locais,
sem segredos reais.

Principais variáveis lidas pelo binário do serviço:

| Variável | Padrão | Significado |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | nome da instância nos logs e leases do outbox |
| `HTTP_ADDR` | `:8080` | endereço de escuta |
| `DATABASE_URL` | — (obrigatório) | DSN de runtime (papel `wallet_app`) |
| `DATABASE_LOCK_TIMEOUT` | `5s` | espera máxima por um lock de linha da carteira |
| `MIGRATION_DATABASE_URL` | — | DSN do proprietário usado pelo `migrate` |
| `AWS_ENDPOINT_URL` | — | endpoint do LocalStack (`http://localstack:4566`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | credenciais do broker para o serviço |
| `SQS_INBOUND_QUEUE` / `SQS_INBOUND_DLQ` / `SQS_EVENTS_QUEUE` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | nomes das filas |
| `SQS_VISIBILITY_TIMEOUT` / `SQS_HANDLER_TIMEOUT` | `30s` / `20s` | o timeout do handler deve ser menor que o de visibilidade |
| `SQS_RETRY_BASE_DELAY` / `SQS_RETRY_MAX_DELAY` | `2s` / `60s` | backoff aplicado a falhas transitórias |
| `SQS_ALLOWED_PROVIDERS` | `provider-a,provider-b` | provedores aceitos da fila |
| `OIDC_ISSUER` | — (obrigatório) | `iss` esperado (`http://localhost:8080/realms/wagering`) |
| `OIDC_JWKS_URL` | issuer + `/protocol/openid-connect/certs` | onde as chaves de assinatura são buscadas |
| `OIDC_AUDIENCE` | `wagering-api` | `aud` esperado |
| `PENDING_MAX_ATTEMPTS` / `PENDING_TTL` | `10` / `10m` | orçamento de uma referência pendente |
| `OUTBOX_LEASE` | `30s` | lease antes que linhas abandonadas do outbox sejam recuperadas |
| `SHUTDOWN_TIMEOUT` | `25s` | orçamento de encerramento gracioso |

[.env.example](.env.example) lista todas elas.

## 4. Filas

As filas são criadas automaticamente por
[deploy/localstack/init-sqs.sh](deploy/localstack/init-sqs.sh), um hook de "ready" do LocalStack:

| Fila | Tipo | Propósito |
| --- | --- | --- |
| `wager-transactions.fifo` | FIFO, visibilidade 30s, **redrive para DLQ após 5 recebimentos** | operações de entrada |
| `wager-transactions-dlq.fifo` | FIFO, retenção de 14 dias | mensagens mortas (redrive + erros permanentes) |
| `wallet-events.fifo` | FIFO | eventos de integração de saída (outbox) |

Para re-executar o provisionamento manualmente (o script é idempotente):

```sh
docker compose exec localstack /etc/localstack/init/ready.d/init-sqs.sh
docker compose exec localstack awslocal sqs list-queues
```

## 5. Migrações

As migrações são arquivos SQL versionados em [migrations/](migrations), no
formato goose com seções `Up` e `Down`. São embutidas nos binários e
executadas pelo `cmd/migrate` com o papel de proprietário.

```sh
# com docker compose (executa automaticamente no `up`):
docker compose run --rm migrate                                         # aplica todas
docker compose run --rm --entrypoint /app/migrate migrate status
docker compose run --rm --entrypoint /app/migrate migrate down          # reverte a última
docker compose run --rm --entrypoint /app/migrate migrate down-to 0     # reverte tudo

# a partir do host (Go instalado):
export MIGRATION_DATABASE_URL='postgres://wagering_owner:wagering_owner@localhost:55432/wagering?sslmode=disable'
go run ./cmd/migrate up | down | down-to N | status
# ou: make migrate-up / make migrate-down / make migrate-status
```

| Versão | Conteúdo |
| --- | --- |
| 00001 | wallets, wager transactions, ledger, todas as constraints e triggers de proteção |
| 00002 | inbox, outbox, seus triggers e permissões para o papel de runtime |

## 6. Executando a aplicação

O Docker Compose já executa três instâncias. Para executar uma instância a partir do código-fonte
contra a infraestrutura do Compose:

```sh
docker compose up -d postgres keycloak localstack migrate
DATABASE_URL='postgres://wallet_app:wallet_app@localhost:55432/wagering?sslmode=disable' \
AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=wallet-service AWS_SECRET_ACCESS_KEY=x \
OIDC_ISSUER=http://localhost:8080/realms/wagering HTTP_ADDR=:8090 INSTANCE_ID=local \
go run ./cmd/wallet-service
```

`SIGTERM`/`SIGINT` acionam um encerramento gracioso (veja ARCHITECTURE.md §13).

## 7. Autenticação e identidades de teste

O Keycloak é provisionado a partir de
[deploy/keycloak/realm-wagering.json](deploy/keycloak/realm-wagering.json)
com clientes confidenciais usando o grant **client_credentials**:

| Client ID | Segredo | Papel | Claim `provider_id` | Uso |
| --- | --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | provedor de jogo A |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | provedor de jogo B |
| `wallet-service` | `wallet-service-secret` | `wallet-operator` | — | serviço interno (operações de carteira) |
| `provider-a-shortlived` | `provider-a-shortlived-secret` | `wagering-provider` | `provider-a` | tokens de 2 segundos (testes de expiração) |
| `no-role-client` | `no-role-client-secret` | — | — | autenticado mas sem permissões |

Console de administração do Keycloak: <http://localhost:8080> (`admin` / `admin`).

```sh
token() {
  curl -s -X POST http://localhost:8080/realms/wagering/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" | jq -r .access_token
}
OP=$(token wallet-service wallet-service-secret)   # serviço interno
PA=$(token provider-a provider-a-secret)           # provedor A
PB=$(token provider-b provider-b-secret)           # provedor B
```

Permissões:

| Endpoint | `wallet-operator` | `wagering-provider` |
| --- | --- | --- |
| `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` | ✅ | ❌ 403 |
| `POST /wagering/transactions` | ❌ 403 | ✅ apenas com `providerId` = próprio `provider_id` |
| `GET /wagering/transactions/{id}` | ✅ qualquer | ✅ apenas suas transações (outras: 404) |
| `GET /providers/{providerId}/wagering/transactions/{ext}` | ✅ qualquer | ✅ apenas próprio `providerId` (outros: 403) |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | público | público |

## 8. API HTTP

Os exemplos usam `app1` (`:8081`); qualquer instância retorna os mesmos resultados.

### Abrir uma carteira

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
Uma segunda carteira para o mesmo jogador e moeda retorna `409 WALLET_ALREADY_EXISTS`.

### Enviar uma operação

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
Enviando novamente, para qualquer instância, retorna o mesmo corpo com
`"idempotentReplay": true`. O saldo é o observado quando a operação foi
processada pela primeira vez, mesmo que a carteira tenha se movido desde então.

Para `REFUND`/`ROLLBACK`, adicione `"referenceExternalTransactionId": "..."` ao corpo.

### Contrato de resposta

| Situação | HTTP | Corpo |
| --- | --- | --- |
| Processado (novo ou replay) | `200` | `{transactionId, status:"PROCESSED", balance, idempotentReplay}` |
| Aguardando referência / pendente | `202` | `{transactionId, status:"PENDING_REFERENCE", nextAttemptAt, idempotentReplay}` |
| Rejeição de negócio (persistida, terminal) | `422` | `{transactionId, status:"REJECTED", failureCode, balance, idempotentReplay}` |
| Entrada inválida (não persistida) | `400` | `{"error":{"code":"VALIDATION_ERROR" \| "INVALID_JSON" \| "MISSING_IDEMPOTENCY_KEY", "message"}, correlationId}` |
| Token ausente / inválido / expirado | `401` | `{"error":{"code":"UNAUTHENTICATED"}}` |
| Não permitido | `403` | `{"error":{"code":"FORBIDDEN"}}` |
| Carteira ou transação desconhecida | `404` | `{"error":{"code":"WALLET_NOT_FOUND" \| "NOT_FOUND"}}` |
| Chave reutilizada com outro payload | `409` | `{"error":{"code":"IDEMPOTENCY_KEY_CONFLICT"}}` |
| ID externo reutilizado com outra chave | `409` | `{"error":{"code":"EXTERNAL_TRANSACTION_CONFLICT"}}` |
| Indisponibilidade transitória (banco caiu, timeout de lock) | `503` + `Retry-After: 1` | `{"error":{"code":"SERVICE_UNAVAILABLE"}}` (reenvie com a mesma chave) |

Exemplos:

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

### Códigos de falha

Os códigos são estáveis. **Erros de entrada** nunca são persistidos: corrija a requisição e
reenvie com a mesma chave. **Rejeições** são persistidas e finais para aquele
`(providerId, externalTransactionId)`: uma operação corrigida precisa de um novo
ID externo.

| `failureCode` | Significado | Natureza |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | um `BET` maior que o saldo | resultado definitivo |
| `REVERSAL_INSUFFICIENT_FUNDS` | um `ROLLBACK` que precisa debitar (reverte um `WIN`/`REFUND`) além do saldo | resultado definitivo |
| `REFERENCE_NOT_FOUND` | a referência nunca chegou dentro do orçamento de retry/TTL | resultado definitivo |
| `REFERENCE_NOT_PROCESSED` | a referência terminou `REJECTED`/`FAILED` (ou ainda estava pendente na expiração) | resultado definitivo |
| `REFERENCE_ALREADY_REVERSED` | a referência já tem um `REFUND`/`ROLLBACK` bem-sucedido | resultado definitivo |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da carteira | erro de dados do cliente (use um novo ID externo) |
| `PLAYER_WALLET_MISMATCH` | a carteira não pertence ao jogador | erro de dados do cliente |
| `REFERENCE_MISMATCH` | provedor/jogador/carteira/moeda/rodada diferem da referência | erro de dados do cliente |
| `REFERENCE_KIND_NOT_ALLOWED` | ex.: `REFUND` de um `WIN`, `ROLLBACK` de um `ROLLBACK` | erro de dados do cliente |
| `REVERSAL_AMOUNT_MISMATCH` | reversão parcial (valor ≠ valor referenciado) | erro de dados do cliente |
| `PROCESSING_FAILED` | falha permanente de infraestrutura ao retomar (status `FAILED`) | auditoria |

### Consultas

```sh
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $OP"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $OP"          # → {entries, nextCursor}
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50&cursor=djE6NDY0" -H "Authorization: Bearer $OP"
curl -s localhost:8081/wagering/transactions/<transactionId> -H "Authorization: Bearer $PA"
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PA"
```

As consultas de transação retornam o status, `failureCode`, `attempts`,
`nextAttemptAt`, `referenceExpiresAt`, o `referenceTransactionId` resolvido
e o `balance` resultante, para que operações pendentes possam ser acompanhadas.

### Reconciliação

```sh
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $OP"
```
```json
{"walletId":"01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052",
 "storedBalance":{"amount":"975.00","currency":"BRL"},"calculatedBalance":{"amount":"975.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

## 9. Enviando operações pelo SQS

```sh
WALLET=01a0da0f-0751-7ce3-9bc7-f1a3e6ca2052
BODY='{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
 "data":{"providerId":"provider-a","externalTransactionId":"transaction-456","idempotencyKey":"provider-a:transaction-456",
 "playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987",
 "gameId":"fortune-chimp","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}'
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 --message-body "$BODY"

# inspecionar o resultado / DLQ / eventos publicados
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-456 -H "Authorization: Bearer $PA"
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo --message-attribute-names All
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo
```

Contrato de roteamento: `MessageGroupId = walletId`, `MessageDeduplicationId = messageId`
(ARCHITECTURE.md §10).

## 10. Testes

| Suite | Necessita | Comando |
| --- | --- | --- |
| Unitários (domínio, money, config, parsing…) | nada | `go test ./...` |
| Unitários com race detector | nada | `go test -race ./...` |
| Verificações estáticas | nada | `go vet ./...` (mais `-tags integration`, `-tags e2e`) |
| Integração: PostgreSQL real, Keycloak e LocalStack; instâncias em processo | `docker compose up -d postgres keycloak localstack` | `go test -race -count=1 -tags integration ./test/integration/...` |
| End-to-end: três processos de serviço | `docker compose up -d --build` | `go test -race -count=1 -tags e2e ./test/e2e/...` |
| Simulação de falha: SIGKILL do `app1` sob carga | stack completo | `E2E_CHAOS=1 go test -count=1 -tags e2e -run Chaos ./test/e2e/...` |

Atalhos: `make test`, `make test-race`, `make test-integration`,
`make test-e2e`, `make test-chaos`, `make vet`.

A suite de integração aplica as migrações por conta própria. Usa o papel restrito
`wallet_app`, como em produção, e cria **suas próprias filas SQS por teste**,
podendo rodar enquanto as instâncias do Compose estão ativas. A suite e2e precisa
das três instâncias do Compose saudáveis.

O que as suites cobrem, mapeado para o §13 do desafio:

| Requisito | Teste |
| --- | --- |
| Parsing de money, escala, limites, overflow, entrada inválida, divergência de moeda | `internal/domain/money/money_test.go` (+ fuzz `FuzzParse`) |
| Invariantes de carteira, transições de estado, 5 tipos, política de zero, abertura + eventos, conflito de payload | `internal/domain/domain_test.go` |
| Migrações up/down, constraints, imutabilidade do ledger, atomicidade | `test/integration/db_test.go` |
| Mesma aposta ×50 em paralelo → um único débito | `TestSameBetFiftyTimesInParallel`, `TestE2ESameBetFiftyTimesAcrossInstances` |
| 100.00 vs duas apostas de 80.00 concorrentes | `TestTwoConcurrentBetsOnLimitedBalance`, `TestE2ETwoBetsRaceAcrossInstances` (10 rodadas cada) |
| Carteiras independentes em paralelo / sem lock global | `TestIndependentWalletsProgressInParallel`, `TestE2EDistinctWalletsInParallel` |
| ≥ 3 instâncias independentes | suite e2e (containers `app1..app3`) + 3 pools na integração |
| Consumidor morto após commit, antes do delete → reentrega | `TestCrashAfterCommitBeforeDelete` |
| Retries, backoff, DLQ, mensagens inválidas | `TestTransientFailuresExhaustToDLQ`, `TestInvalidMessagesGoToDLQ` |
| Dois publishers competindo + recuperação entre publicação e confirmação | `TestOutboxCompetingPublishersAndRecovery`, `TestE2EOutboxPublishes` |
| Reversão antes de sua referência: resolução / expiração | `TestReversalBeforeReferenceIsResolvedLater`, `TestPendingReferenceExpires`, `TestE2EReversalBeforeReferenceAcrossInstances` |
| Reinicialização preserva idempotência, trabalho pendente e consistência | `TestRestartPreservesPendingAndIdempotency`, `TestResumeCrashedPending`, `TestIdempotencyRules`, `TestE2EChaosKillInstance` |
| Mesma operação via HTTP e SQS | `TestSameOperationThroughHTTPAndSQS`, `TestE2EHTTPAndSQSSameOperation` |
| Início/parada do Fx, encerramento de workers, liberação de recursos | `TestFxLifecycle` |
| IdP real; credenciais ausentes/inválidas/expiradas; isolamento de provedores; sem efeito financeiro | `TestAuthentication`, `TestAuthorizationAndProviderIsolation` |
| Saldo armazenado = créditos − débitos ao final | `AssertConsistent` / `reconcile` nos testes |

## 11. Observabilidade

- **Logs**: JSON (`log/slog`) no stdout. Cada linha carrega os identificadores
  disponíveis: `instance`, `correlationId` (header `X-Correlation-Id`, ou o
  `messageId` do SQS), `messageId`, `transactionId`, `walletId`, `providerId`.
  Tokens e payloads financeiros completos nunca são registrados.
- **Métricas**: Prometheus em `GET /metrics`.

| Métrica | Labels | O que mede |
| --- | --- | --- |
| `wagering_transactions_total` | kind, status, source | resultados por status |
| `wagering_duplicates_total` | source, mechanism (`idempotency`/`inbox`) | duplicatas detectadas |
| `wagering_retries_total` | component | retries (db_tx, sqs, sqs_receive, outbox, pending) |
| `wagering_sqs_dlq_total` | reason | mensagens enviadas para a DLQ |
| `wagering_concurrency_conflicts_total` | reason | corridas resolvidas por retry |
| `wagering_outbox_lag_seconds` | — | idade do evento não publicado mais antigo |
| `wagering_outbox_published_total` / `wagering_outbox_publish_failures_total` | event_type | entrega via outbox |
| `wagering_processing_duration_seconds` | source | histograma de latência de processamento |
| `wagering_reconciliation_divergences_total` | — | divergências de reconciliação |
| `wagering_pending_reference_resolutions_total` | outcome | resultados de referências pendentes |

- **Health**: `GET /health/live` (processo) e `GET /health/ready` (ping do PostgreSQL
  + `GetQueueAttributes` do SQS; retorna 503 enquanto drena no encerramento).

## 12. Estrutura do projeto

```
cmd/wallet-service     ponto de entrada do serviço (fx.New(...).Run())
cmd/migrate            CLI de migração (up, down, down-to, status)
internal/domain        entidades, máquina de estados, regras de negócio, eventos (sem imports de infra)
internal/domain/money  objeto de valor Money (int64 unidades menores)
internal/app           casos de uso + ports (UnitOfWork, repositórios)
internal/postgres      adaptador pgx: SQL, transações, locks, classificação de erros, migrator
internal/messaging     cliente SQS, consumidor de entrada, publicador de eventos
internal/worker        loops gerenciados por ciclo de vida, relay do outbox
internal/auth          validação de access tokens OIDC, principal/papéis
internal/httpapi       handlers net/http, middleware, ciclo de vida do servidor
internal/observability logger JSON slog, métricas Prometheus
internal/bootstrap     módulos Fx e ordenação do ciclo de vida
migrations             SQL versionado (goose), embutido
deploy                 realm Keycloak, init LocalStack, init papel PostgreSQL
test/integration       suite de integração (build tag integration)
test/e2e               suite multi-processo (build tag e2e)
```
