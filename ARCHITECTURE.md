# Architecture and decisions

This document records the technical decisions, the guarantees they provide and
how each one is verified. It also lists the interpretations I adopted and the
known limitations.

## Contents

1. [Overview](#1-overview)
2. [Packages and dependency rule](#2-packages-and-dependency-rule)
3. [Money](#3-money)
4. [Persistence and transaction boundaries](#4-persistence-and-transaction-boundaries)
5. [Invariants enforced by the database](#5-invariants-enforced-by-the-database)
6. [Concurrency control](#6-concurrency-control)
7. [Idempotency](#7-idempotency)
8. [Transaction state machine and failure classification](#8-transaction-state-machine-and-failure-classification)
9. [Operations, references and reversals](#9-operations-references-and-reversals)
10. [SQS consumer and inbox](#10-sqs-consumer-and-inbox)
11. [Transactional outbox](#11-transactional-outbox)
12. [Authentication and authorization](#12-authentication-and-authorization)
13. [Uber Fx composition, lifecycle and shutdown](#13-uber-fx-composition-lifecycle-and-shutdown)
14. [Observability](#14-observability)
15. [Failure scenarios](#15-failure-scenarios)
16. [Interpretations, limitations and unfinished work](#16-interpretations-limitations-and-unfinished-work)

## 1. Overview

A single binary runs the HTTP API, the SQS consumer, the outbox relay and the
pending-reference worker. Any number of identical instances run against the
same PostgreSQL and SQS; Compose starts three. Instances share no memory and
hold no local locks. All coordination goes through PostgreSQL (row locks,
constraints, `SKIP LOCKED` leases) and SQS (visibility, redrive).

Every operation is decided in **one SQL transaction**. That transaction:

- locks the wallet row,
- re-checks idempotency,
- applies the domain rules,
- writes the transaction, the new balance, the ledger entry, the outbox events
  and (for SQS) the inbox record.

Nothing is published or acknowledged before that commit.

## 2. Packages and dependency rule

```
cmd ──▶ bootstrap (Fx) ──▶ httpapi, messaging, worker, auth, postgres, observability
                                   │            │          │
                                   ▼            ▼          ▼
                                  app (use cases + ports) ◀─ postgres implements ports
                                   │
                                   ▼
                                domain, domain/money   (std lib + google/uuid only)
```

- `internal/domain` has no knowledge of Fx, HTTP, SQS or pgx. Entities keep
  their state in unexported fields and change only through validated methods.
  Constructors (`OpenWallet`, `NewExternalTransaction`, `NewLedgerEntry`) are
  separate from rehydration (`RehydrateWallet`, `RehydrateTransaction`), which
  validates but never replays movements, transitions or events.
- `domain.Settle` is a pure function holding all business rules of the five
  kinds. It mutates the transaction and the wallet in memory and returns the
  ledger entry and the events. The whole rule set is unit-tested without a
  database.
- `internal/app` declares the ports (`UnitOfWork`, repositories, outbox store,
  publisher). HTTP and SQS call the same `WageringService.Submit`.
- Domain errors can be classified with `errors.Is`/`errors.As`:
  `ErrInvalidArgument`, `ErrInvalidTransition` (via `*TransitionError`),
  `ErrInvariantViolation`, and `*RejectionError{Code}`. Business rejections
  never `panic`. Every I/O function takes a `context.Context`.

## 3. Money

- **Representation**: `money.Money{units int64, currency Currency}`. Amounts
  are counted in minor units (cents) with a fixed scale of 2. Only ISO 4217
  currencies with 2 minor digits are accepted (`BRL`, `USD`, `EUR`, …). The
  zero value is invalid, and every operation rejects it with `ErrUninitialized`.
- **Limits**: from −92,233,720,368,547,758.08 to 92,233,720,368,547,758.07.
  Parsing, `Add`, `Sub` and `Neg` detect overflow and return `ErrOverflow`.
  `Neg(MinInt64)` is covered.
- **Parsing** is done digit by digit on the string, with no `float` anywhere.
  The grammar is `-?(0|[1-9][0-9]*)(\.[0-9]{1,2})?`. It rejects empty strings,
  `NaN`, `Infinity`, scientific notation, a leading `+`, leading zeros, a
  trailing `.`, and more than 2 decimals (`ErrScaleExceeded`: never rounded).
  External inputs use `ParseNonNegative`, which rejects negatives. Currency
  codes must be upper case.
- **Accepted equivalent forms**: `"25"`, `"25.5"` and `"25.50"`. Before
  hashing and persisting, they are normalized to exactly two decimals
  (`"25.00"`, `"25.50"`).
- **Serialization**: always `{"amount":"25.00","currency":"BRL"}`. The amount
  is a JSON *string*, and a JSON number is refused at decoding time.
- **Persistence**: `BIGINT` minor units plus `CHAR(3)` currency in every table
  (`balance_minor`, `amount_minor`, `balance_before_minor`, …). The mapping
  goes through `money.FromUnits`. Aggregations (reconciliation) sum in
  `NUMERIC` and cast back to `BIGINT`, so an overflow errors instead of
  wrapping.
- Arithmetic and comparison require the same currency (`ErrCurrencyMismatch`).
  Negative values are allowed for internal differences (e.g. reconciliation
  `difference`), never for balances.

## 4. Persistence and transaction boundaries

- **Library**: `pgx/v5` (`pgxpool`) with explicit SQL, no ORM. Locks
  (`FOR UPDATE`, `SKIP LOCKED`), compare-and-set updates and constraint names
  are all visible in `internal/postgres`.
- **Unit of work**: `UnitOfWork.Run(ctx, fn)` opens one `READ COMMITTED`
  transaction. Every repository reached through the `Tx` argument (`Wallets()`,
  `Transactions()`, `Ledger()`, `Outbox()`, `Inbox()`) shares it, so their
  writes commit or roll back together. `UnitOfWork.Snapshot` opens a
  `REPEATABLE READ READ ONLY` transaction, which reconciliation uses.
- **Error classification** (`postgres.mapErr`):

  | Error | Class | Handling |
  | --- | --- | --- |
  | unique violation, deadlock, serialization failure | `ErrRetryableConflict` | the use case retries the whole unit up to 5 times with jitter; the retry then observes the winner and becomes a replay |
  | version CAS miss | `ErrConcurrentUpdate` | same as above |
  | lock timeout (`55P03`), cancel, admin shutdown, connection errors, commit outcome unknown | `ErrTransient` | HTTP 503 with `Retry-After`; SQS visibility backoff |
  | check violations, trigger exceptions, anything else | permanent | HTTP 500 / SQS retry until redrive to the DLQ / worker marks `FAILED` |

- **Time**: the application clock (UTC) timestamps domain facts. Outbox leases
  use the database `now()`, so instance clock skew cannot break them.
- **Migrations**: goose SQL files with `Up`/`Down` sections, embedded and run
  by `cmd/migrate` with the owner role. The runtime role `wallet_app` only
  gets `SELECT, INSERT` on the ledger and inbox (no `UPDATE`/`DELETE`).

## 5. Invariants enforced by the database

The schema enforces the invariants on its own, independently of application
locks and of SQS FIFO deduplication:

| Invariant | Mechanism |
| --- | --- |
| Non-negative balance | `CHECK (balance_minor >= 0)`; ledger balances also `>= 0` |
| One wallet per (player, currency) | `UNIQUE (player_id, currency)` |
| Every balance change has its ledger entry in the same commit | `DEFERRABLE INITIALLY DEFERRED` constraint trigger `wallets_ledger_consistency`: at commit, each balance change must match an entry with the same `wallet_version`, `balance_before` and `balance_after` |
| Version starts at 1 and grows by exactly 1 per balance change | `wallets_guard` trigger |
| No lost update / no forked history | `UNIQUE (wallet_id, wallet_version)` on the ledger + version CAS + row lock |
| Ledger arithmetic | `CHECK (after = before ± amount)` by direction |
| One ledger entry per transaction | `UNIQUE (wallet_id, transaction_id)` |
| Ledger is append-only | `BEFORE UPDATE OR DELETE` and `BEFORE TRUNCATE` triggers + no privileges for the runtime role |
| Idempotency | `UNIQUE (provider_id, external_transaction_id)` and `UNIQUE (provider_id, idempotency_key)` for external transactions |
| No duplicate opening credit | partial `UNIQUE (wallet_id) WHERE kind='OPENING'` |
| At most one successful reversal per reference | partial `UNIQUE (reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'` |
| Internal vs external shape | `CHECK` on `origin`: OPENING has no provider/external id/key/hash/round/game/reference; external rows require them |
| Zero-amount policy | `CHECK ((kind='LOSS' AND amount=0) OR (kind<>'LOSS' AND amount>0))` |
| Terminal transactions are final; identity/payload immutable; no delete | `wager_transactions_guard` trigger |
| Outbox snapshot immutable; published stays published | `outbox_events_guard` trigger |
| Inbox is immutable; one row per (consumer, message) | trigger + `PRIMARY KEY (consumer_name, message_id)` |

`test/integration/db_test.go` exercises each of these with raw SQL, bypassing
the application.

## 6. Concurrency control

**Strategy: pessimistic row lock per wallet, plus an optimistic version
compare-and-set, plus database constraints.**

1. `SELECT … FROM wallets WHERE id = $1 FOR UPDATE` serializes the writers of
   one wallet. Other wallets are unaffected, and there is no global lock.
   `TestIndependentWalletsProgressInParallel` holds one wallet's lock while
   30 other wallets keep processing.
2. `UPDATE wallets SET … WHERE id = $1 AND version = $expected` is a second,
   independent guard against lost updates.
3. `UNIQUE (wallet_id, wallet_version)` and the deferred consistency trigger
   are the last line of defense, even if both application guards had a bug.

Why not `SERIALIZABLE` or pure optimistic locking? A hot wallet under
optimistic locking produces retry storms. `SERIALIZABLE` aborts transactions
on predicate conflicts unrelated to the wallet. A row lock gives
deterministic queueing per wallet, and `lock_timeout` (5s) bounds the wait,
surfacing as a transient 503/retry.

**Duplicates under concurrency**: a quick idempotency lookup runs without the
lock. If nothing is found, the wallet is locked and the lookup is repeated.
Concurrent duplicates target the same wallet, so they queue on that lock and
see the committed row. A duplicate carrying a different `walletId` loses on
the unique index, retries, and becomes a replay or a conflict.

**Lock ordering** (deadlock avoidance):

- `Submit` takes inbox lookup → wallet lock.
- The pending worker takes the wallet lock with `SKIP LOCKED`, then reads the
  transaction.

Nothing locks a transaction row before a wallet.

**Mandatory scenario** (100.00 BRL, two concurrent 80.00 bets): one
`PROCESSED`, one `REJECTED/INSUFFICIENT_FUNDS`, final 20.00, a single debit.
Resends change nothing. It is verified 10 times in-process with 3
independent pools, and 10 times across the three Compose processes.

## 7. Idempotency

- **Key**: the `Idempotency-Key` header (HTTP) or `data.idempotencyKey` (SQS)
  is mandatory and stored exactly as received. The server never substitutes
  a computed key. Keys are scoped by provider:
  `UNIQUE (provider_id, idempotency_key)`.
- **Payload hash**: SHA-256 (hex) over a canonical JSON with lexicographically
  sorted keys and no whitespace. The fields are `externalTransactionId`,
  `gameId`, `kind`, `money{amount,currency}`, `playerId`, `providerId`,
  `referenceExternalTransactionId` (only when present), `roundId` and
  `walletId`. Normalizations: UUIDs in lower-case canonical form, amount with
  exactly two decimals, upper-case currency. **Excluded**: the idempotency
  key, `messageId`, correlation ids, timestamps and headers. HTTP and SQS
  build the same `RawExternalRequest`, so one operation gets the same hash
  from both entry points (unit-tested in `messaging/message_test.go`).
- **Rules** (`WageringService.findExisting`):

  | Situation | Result |
  | --- | --- |
  | same key, same hash | replay: the persisted result with `idempotentReplay: true` |
  | same key, different hash | `409 IDEMPOTENCY_KEY_CONFLICT` |
  | same `(providerId, externalTransactionId)` with another key | `409 EXTERNAL_TRANSACTION_CONFLICT` (never reapplied) |

- **Replay result**: `result_balance_minor` stores the balance observed when
  the operation concluded (or was rejected), so a replay returns the original
  balance even after later movements. A replay of a pending transaction
  returns `202` with its current status.
- **Durability**: all of this lives in PostgreSQL and survives restarts of
  every process (`TestIdempotencyRules` uses a fresh pool; the e2e chaos test
  kills a process).

## 8. Transaction state machine and failure classification

```
PENDING ──────────────┬──▶ PROCESSED  (terminal)
   │                  ├──▶ REJECTED   (terminal, failureCode)
   ▼                  └──▶ FAILED     (terminal, failureCode)
PENDING_REFERENCE ────┘
   ▲  │  reschedule (attempts++, nextAttemptAt moves forward)
   └──┘
```

- Transitions are methods (`MarkPendingReference`, `MarkProcessed`,
  `MarkRejected`, `MarkFailed`). From a terminal state every one of them
  returns `*TransitionError` (`errors.Is(err, ErrInvalidTransition)`). The
  database trigger refuses them too.
- **Synchronous path**: operations without a pending dependency go from
  `PENDING` to a terminal state in memory, inside the same transaction, with
  no intermediate commit. A `PENDING` row is therefore never committed by the
  normal paths. The worker nevertheless resumes any committed `PENDING` row
  (for example, one left by a crashed future asynchronous path), as
  `TestResumeCrashedPending` shows.
- **Transient vs permanent**:
  - **Transient**: database or broker unavailability, lock timeout, deadline,
    concurrency races. These are retried: an immediate bounded retry for
    races, then 503 over HTTP, SQS visibility backoff, or the next worker
    poll. No state is written.
  - **Permanent business outcome**: `REJECTED` with a `failureCode`; persisted
    and terminal.
  - **Permanent infrastructure failure**: while resuming a persisted
    transaction, a non-transient error such as a violated constraint or
    corrupt data. The transaction is marked `FAILED` with `PROCESSING_FAILED`
    for audit, and no event is emitted.
  - **Invalid input**: never persisted. HTTP answers 400/404; SQS sends the
    message to the DLQ.

## 9. Operations, references and reversals

| Kind | Movement | Rules |
| --- | --- | --- |
| `BET` | debit | amount > 0; balance ≥ amount, else `INSUFFICIENT_FUNDS` |
| `WIN` | credit | amount > 0; optional reference must be a `PROCESSED BET` of the same round |
| `LOSS` | none | amount must be `0.00`; wallet currency required; no ledger entry, no version change; emits `WagerTransactionProcessed` only |
| `REFUND` | credit | reference required; must be a `PROCESSED BET`; same amount |
| `ROLLBACK` | opposite of the reference | reference required; `BET` → credit, `WIN`/`REFUND` → debit; same amount; a debit beyond the balance is `REVERSAL_INSUFFICIENT_FUNDS` (distinct from `INSUFFICIENT_FUNDS`, auditable) |
| `OPENING` | credit | internal only; rejected when received via HTTP or SQS |

- A reference is resolved by `(providerId, referenceExternalTransactionId)`.
  Provider, player, wallet, currency and round must all match
  (`REFERENCE_MISMATCH`). Partial reversals are refused
  (`REVERSAL_AMOUNT_MISMATCH`).
- **Combining `REFUND` and `ROLLBACK` on the same bet**: a referenced
  transaction accepts **at most one successful reversal in total**, whatever
  its kind. This is stricter than "one per kind". It prevents returning the
  same debit twice, for example with a `REFUND` followed by a `ROLLBACK` of
  the same `BET`. The partial unique index enforces it under concurrency
  (`TestConcurrentReversalsOfSameBet`). A `ROLLBACK` of a `REFUND` is allowed:
  it undoes the refund and re-debits. After that the original `BET` cannot be
  refunded again, which is conservative and never over-credits. A `ROLLBACK`
  of a `ROLLBACK` is refused (`REFERENCE_KIND_NOT_ALLOWED`).
- **Zero policy**: zero is accepted only for the initial balance and for
  `LOSS`. The domain, the request validation and a `CHECK` constraint all
  enforce it.

### References not yet available

- A missing reference makes the operation `PENDING_REFERENCE`. The operation
  is committed, emits `WagerTransactionPendingReference`, and gets
  `nextAttemptAt` and `referenceExpiresAt = createdAt + TTL`. The client
  receives `202`.
- The **pending-reference worker** runs on every instance and survives
  restarts, because its state lives in the table. It lists due transactions
  and, for each one, locks the wallet with `FOR UPDATE SKIP LOCKED` (wallets
  busy elsewhere are skipped until the next poll). It then re-reads the
  transaction, checks it is still open and due, and runs `Settle` again.
- **Backoff**: exponential. It starts at 500ms and doubles up to a 1-minute
  cap (`PENDING_BASE_BACKOFF`, `PENDING_MAX_BACKOFF`), with jittered polling.
  `attempts` is persisted, so the budget survives restarts.
- **Expiry**: after `PENDING_MAX_ATTEMPTS` (10, counting the first synchronous
  attempt) or `PENDING_TTL` (10 minutes), the operation becomes `REJECTED`
  with `REFERENCE_NOT_FOUND`, and `WagerTransactionRejected` is emitted.
- **Reference exists but is still pending** (`PENDING`/`PENDING_REFERENCE`):
  the operation waits with the same backoff and budget, and expires with
  `REFERENCE_NOT_PROCESSED`.
- **Reference ended without success** (`REJECTED`/`FAILED`): the operation is
  immediately `REJECTED` with `REFERENCE_NOT_PROCESSED`.

## 10. SQS consumer and inbox

- **Durable message identity**: the envelope `messageId`. The inbox key is
  `(consumer_name, message_id)`, and the inbox hash is
  `SHA-256(type + "\n" + idempotencyKey + "\n" + payloadHash)`.
- **Atomicity**: the inbox record is inserted in the **same SQL transaction**
  as the transaction row, balance, ledger entry and outbox events. For a
  pending reference, the inbox row commits with the `PENDING_REFERENCE`
  record, and the worker takes over from there.
- **Redelivery**: a known `messageId` with the same hash is acknowledged and
  returns the stored result. A different hash gives `ErrMessageConflict` and
  the message goes to the DLQ. A new `messageId` for an already registered
  operation is caught by the idempotency rules.
- **Deletion only after commit**. `TestCrashAfterCommitBeforeDelete` makes
  the consumer "die" between the commit and `DeleteMessage`. The message
  comes back (receive count 2) to an independent consumer, which acknowledges
  it through the inbox with a single debit.
- **Outcomes**:

  | Outcome | When | Broker action |
  | --- | --- | --- |
  | ack | processed, business rejection, pending reference, duplicate | `DeleteMessage` |
  | retry | transient or unknown error | `ChangeMessageVisibility` to `min(2s·2^(receiveCount−1), 60s)` |
  | DLQ | invalid JSON/envelope, unknown type/fields, `OPENING`, validation error, provider not in `SQS_ALLOWED_PROVIDERS`, wallet not found, idempotency/message conflicts | copy to `wager-transactions-dlq.fifo` with `failureReason`/`failureDetail` attributes, then delete |
  | release | shutdown interrupted handling | visibility → 0, so another instance takes it immediately |

- **Limits**: visibility timeout 30s; handler timeout 20s (validated to be
  lower than the visibility); **`maxReceiveCount` = 5**, after which the
  redrive policy moves the message to the DLQ; up to 10 messages per receive;
  long polling of 10s.
- **Group and deduplication ids**:
  - Producers must send `MessageGroupId = walletId`. This orders each
    wallet's messages and lets different wallets proceed in parallel (the
    queue uses `DeduplicationScope=messageGroup` and
    `FifoThroughputLimit=perMessageGroupId`).
  - `MessageDeduplicationId = messageId`. The broker's 5-minute dedup is only
    an optimization; the inbox is the durable deduplication.
  - Within a received batch, messages of one group are handled sequentially
    and different groups concurrently.
  - Out-of-order arrival (a reversal before its bet) is handled by the domain
    through `PENDING_REFERENCE`, not by relying on FIFO ordering.
- **HTTP vs SQS concurrency**: both paths end in `Submit` and serialize on the
  wallet lock. The loser becomes a replay (`TestSameOperationThroughHTTPAndSQS`,
  `TestE2EHTTPAndSQSSameOperation`, which also sends the same message 3 times
  with different broker dedup ids to force the application dedup).
- **SIGTERM**:
  1. Polling stops at once.
  2. In-flight handlers finish within the stop deadline.
  3. If the deadline expires, handlers are cancelled (their transactions roll
     back) and their messages are released with visibility 0.

  Broker bookkeeping calls (delete, visibility changes) use a context detached
  from shutdown.

## 11. Transactional outbox

- Events are rows in `outbox_events`, written in the same transaction as their
  cause. The relay can only see committed rows, so **nothing is published
  before the commit**.
- **Relay**: runs on every instance. It claims batches with
  `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, setting
  `locked_by`, `locked_until = now() + lease` and `attempts++`. Concurrent
  publishers never claim the same row at the same time.
  - Failures: `next_attempt_at = now + min(1s·2^(attempts−1), 5m)` and the
    lease is released.
  - Abandoned work: when an instance dies holding a lease, the lease expires
    (30s) and another instance claims the rows.
- **Crash windows**:
  - *After commit, before publish*: the row stays unpublished and is claimed
    later.
  - *After publish, before confirmation*: the lease expires and the row is
    republished **with the same `eventId`**. The id comes from the immutable
    stored snapshot and is used as the FIFO `MessageDeduplicationId`.
  - `TestOutboxCompetingPublishersAndRecovery` simulates the second window
    with two competing relays and checks that every event is published, that
    the crashed events were delivered with their original ids, and that
    `attempts ≥ 2`.
- **Delivery semantics**: at-least-once. Consumers must deduplicate by
  `eventId`.
- **Routing contract** (`wallet-events.fifo`): `MessageGroupId = aggregateId`
  (the wallet id for `WalletBalanceChanged`, the transaction id for the
  transaction events), `MessageDeduplicationId = eventId`, and message
  attributes `eventType` and `eventId`. Ordering across aggregates, and
  across instances after a retry, is not guaranteed. Consumers of balance
  events should order by `walletVersion`.
- **Envelope** (built by typed constructors that fix `eventType` and
  `version`; UTC RFC 3339 timestamps; money as decimal strings; the stored
  payload is an immutable snapshot):

```json
{
  "eventId": "01a0…", "eventType": "WalletBalanceChanged", "aggregateType": "Wallet",
  "aggregateId": "<walletId>", "correlationId": "<request or message correlation>",
  "causationId": "<transactionId>", "occurredAt": "2026-09-08T12:00:00.000Z", "version": 1,
  "data": {
    "walletId": "…", "transactionId": "…", "direction": "DEBIT",
    "money": {"amount": "25.00", "currency": "BRL"},
    "balanceBefore": {"amount": "1000.00", "currency": "BRL"},
    "balanceAfter": {"amount": "975.00", "currency": "BRL"},
    "walletVersion": 2
  }
}
```

| Event | Aggregate | Trigger | Data (besides common transaction fields) |
| --- | --- | --- | --- |
| `WagerTransactionProcessed` | transaction | success, including `LOSS` and `OPENING` | `balanceAfter`, `processedAt` |
| `WagerTransactionRejected` | transaction | definitive business rejection | `failureCode`, `rejectedAt` |
| `WalletBalanceChanged` | wallet | effective balance change | as above |
| `WagerTransactionPendingReference` | transaction | first move to `PENDING_REFERENCE` | `nextAttemptAt`, `referenceExpiresAt` |

The common transaction fields are `transactionId`, `origin`, `kind`,
`status`, `walletId`, `playerId`, `money`, plus `providerId`,
`externalTransactionId`, `roundId`, `gameId`, `referenceExternalTransactionId`
and `referenceTransactionId` when they apply. These external fields are
omitted for the internal `OPENING`.

## 12. Authentication and authorization

- **IdP: Keycloak** (in Compose; realm, clients, roles and mappers are
  imported from `deploy/keycloak/realm-wagering.json`). It is a standard
  OAuth 2.0/OIDC server with JWKS key rotation and the `client_credentials`
  grant for service-to-service calls. It supports per-client *hard-coded
  claim* mappers, which is how each provider identity is bound to its
  credentials. The service never stores passwords and never issues tokens.
- **Token validation** (`internal/auth`, `go-oidc`):
  - RS256 signature against the realm JWKS (cached, refreshed on unknown
    `kid`);
  - `iss` equal to the public issuer; `aud` containing `wagering-api` (an
    audience mapper on each client); `exp` checked;
  - `typ` must be `Bearer`, which rejects ID and refresh tokens.

  In Compose the JWKS is fetched through the internal hostname, while `iss`
  is the public URL (`KC_HOSTNAME`).
- **Permission model**: realm roles in `realm_access.roles`.
  - `wagering-provider`: the provider identity is **only** the `provider_id`
    claim. The body's `providerId` must equal it (403 otherwise), so a
    provider cannot submit or replay on behalf of another one. Idempotency
    keys and external ids are scoped by provider in the schema. Reading
    another provider's transaction by id returns 404 (existence is not
    disclosed); the provider-scoped route returns 403.
  - `wallet-operator` (the internal service): wallet endpoints, the ledger,
    reconciliation, reading any transaction. It cannot submit provider
    operations.
  - A token without these roles gets 403. A missing, invalid, tampered or
    expired token gets 401.
- Rejected requests never reach a use case, so they cause no financial effect
  and expose no data. `TestAuthentication` and
  `TestAuthorizationAndProviderIsolation` verify this against the real
  Keycloak, including 2-second tokens used after they expire.
- **Messaging access**:
  - The service authenticates to SQS with its own credentials.
  - The queue policies (provisioned in `init-sqs.sh`) allow only the
    provider principals to `SendMessage` to the inbound queue, and only the
    `wallet-service` principal to consume it, use the DLQ and publish events.
  - The consumer still applies every domain validation and a provider
    allow-list (`SQS_ALLOWED_PROVIDERS`).

## 13. Uber Fx composition, lifecycle and shutdown

- **Modules** (`internal/bootstrap`): `core` (config validation, logger,
  metrics), `postgres`, `sqs`, `app`, `auth`, `http` and `workers`. They use
  constructor injection with `fx.Provide`, and `fx.Annotate`/`fx.As` to bind
  adapters to ports. A single `fx.Invoke(registerLifecycle)` appends the
  entry-point hooks in an explicit order.
- **Start**:
  1. Configuration is validated (required values, handler timeout lower than
     the visibility timeout, …).
  2. The pool pings PostgreSQL with backoff up to the connect timeout.
  3. Queue URLs are resolved.
  4. The workers start.
  5. The HTTP server binds its port synchronously, so a busy port fails fast.
- **Stop** (reverse order, bounded by `SHUTDOWN_TIMEOUT` = 25s; Compose
  `stop_grace_period` is 30s):
  1. The HTTP server reports not-ready (503 on `/health/ready`), stops
     accepting connections and drains in-flight requests.
  2. The SQS consumer stops polling and finishes or releases in-flight
     messages.
  3. The outbox relay loop stops. An interrupted batch is recovered through
     lease expiry, and published events are still confirmed thanks to a
     detached bookkeeping context.
  4. The pending worker stops. An interrupted unit of work rolls back.
  5. The PostgreSQL pool is closed.

  Dependencies close only after every component that uses them has finished.
  This was verified from the logs of `docker compose stop app2`.
- **Workers** (`worker.Loop`) own their context, stop by cancellation, and
  expose `Done()` for observable termination. `TestFxLifecycle` starts and
  stops the whole graph with `fxtest` and asserts that the loops finished,
  that the pool refuses pings, and that the port is closed.

## 14. Observability

- **Logs**: JSON via `log/slog`, with contextual attributes injected by a
  handler (`observability.WithLogFields`): `correlationId`, `messageId`,
  `sqsMessageId`, `transactionId`, `walletId`, `providerId`, `instance`.
- **Metrics**: listed in the README. They cover results by status,
  duplicates, retries, DLQ, concurrency conflicts, outbox lag, latency, and
  reconciliation divergences (which are also logged at ERROR level and
  returned in the response).
- **Health checks**: liveness, plus readiness of PostgreSQL and SQS.
- Tracing (OpenTelemetry) and dashboards were not implemented; see §16.

## 15. Failure scenarios

| Scenario (challenge §3) | Handling | Evidence |
| --- | --- | --- |
| Same operation received repeatedly (HTTP and SQS) | inbox + provider-scoped unique key/external id + lock re-check | `TestSameBetFiftyTimesInParallel`, `TestSameOperationThroughHTTPAndSQS`, e2e equivalents |
| Reversal before its reference | `PENDING_REFERENCE` + durable worker with backoff/TTL | `TestReversalBeforeReferenceIsResolvedLater`, `TestPendingReferenceExpires` |
| Concurrent operations on one wallet | row lock + CAS + constraints | `TestTwoConcurrentBetsOnLimitedBalance`, `TestManyConcurrentMixedOperationsOnOneWallet` |
| Abrupt stop before commit | the transaction rolls back; the client/SQS retries with the same key | `TestFinancialAtomicity`, `TestE2EChaosKillInstance` |
| Abrupt stop after commit | replay by key / inbox; the message is redelivered and acknowledged | `TestCrashAfterCommitBeforeDelete`, chaos test |
| Repeated publication of an event | stable `eventId` + FIFO dedup + consumer dedup | `TestOutboxCompetingPublishersAndRecovery` |
| PostgreSQL or SQS temporarily unavailable | transient classification → 503 / visibility backoff / redrive; start waits for dependencies; readiness fails | `TestTransientFailuresExhaustToDLQ`, readiness checks |

## 16. Interpretations, limitations and unfinished work

- **Broker IAM enforcement**: LocalStack Community stores queue policies but
  does not enforce IAM. The policies are provisioned as they would be on AWS,
  but locally any credentials can call SQS. The domain validations and the
  provider allow-list still apply in the consumer. SQS messages do not carry
  a provider identity that is verified per message. On AWS, a queue per
  provider, or `aws:SourceArn`/principal conditions, would bind the identity
  to the channel.
- **Rejected versus not persisted**: operations whose wallet does not exist,
  and requests that fail validation, are not persisted (HTTP 400/404, SQS
  DLQ). Only rejections of well-formed operations on an existing wallet
  become `REJECTED` rows.
- **`FAILED`** is produced only by the pending worker on non-transient
  errors. Unexpected permanent errors in the synchronous paths are not
  persisted: HTTP returns 500, and SQS retries until the message is
  redriven to the DLQ.
- **Reversal policy** is stricter than required: one successful reversal per
  reference in total (see §9).
- **`WIN` with a reference** waits for the reference like the reversals do
  (`PENDING_REFERENCE`). Without a reference it is processed immediately.
- **The rejection balance** returned to the provider is the balance observed
  when the operation was rejected.
- **`POST /wallets`** has no idempotency key: a repeated opening for the same
  player and currency returns `409`, as the challenge specifies.
- **Outbox ordering** is best-effort (see §11).
- **Double-entry ledger, OpenTelemetry tracing, dashboards and load tests**
  (all optional) were not implemented.
- The "three independent processes" requirement is covered by the e2e suite
  against the Compose containers `app1..app3`. The integration suite also uses
  three independent pools and service instances in one test process, for
  speed and to run under `-race`.
