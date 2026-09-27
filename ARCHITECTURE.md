# Arquitetura e decisões

Este documento registra as decisões técnicas, as garantias que elas fornecem e
como cada uma é verificada. Também lista as interpretações adotadas e as
limitações conhecidas.

## Índice

1. [Visão geral](#1-visão-geral)
2. [Pacotes e regra de dependência](#2-pacotes-e-regra-de-dependência)
3. [Dinheiro](#3-dinheiro)
4. [Persistência e limites de transação](#4-persistência-e-limites-de-transação)
5. [Invariantes aplicados pelo banco de dados](#5-invariantes-aplicados-pelo-banco-de-dados)
6. [Controle de concorrência](#6-controle-de-concorrência)
7. [Idempotência](#7-idempotência)
8. [Máquina de estados da transação e classificação de falhas](#8-máquina-de-estados-da-transação-e-classificação-de-falhas)
9. [Operações, referências e reversões](#9-operações-referências-e-reversões)
10. [Consumidor SQS e inbox](#10-consumidor-sqs-e-inbox)
11. [Outbox transacional](#11-outbox-transacional)
12. [Autenticação e autorização](#12-autenticação-e-autorização)
13. [Composição Uber Fx, ciclo de vida e encerramento](#13-composição-uber-fx-ciclo-de-vida-e-encerramento)
14. [Observabilidade](#14-observabilidade)
15. [Cenários de falha](#15-cenários-de-falha)
16. [Interpretações, limitações e trabalho não concluído](#16-interpretações-limitações-e-trabalho-não-concluído)

## 1. Visão geral

Um único binário executa a API HTTP, o consumidor SQS, o relay do outbox e o
worker de referências pendentes. Qualquer número de instâncias idênticas roda
contra o mesmo PostgreSQL e SQS; o Compose inicia três. As instâncias não
compartilham memória e não mantêm locks locais. Toda a coordenação passa pelo
PostgreSQL (locks de linha, constraints, leases com `SKIP LOCKED`) e pelo SQS
(visibilidade, redrive).

Cada operação é decidida em **uma única transação SQL**. Essa transação:

- bloqueia a linha da carteira,
- reverifica idempotência,
- aplica as regras de domínio,
- grava a transação, o novo saldo, a entrada do ledger, os eventos do outbox
  e (para SQS) o registro do inbox.

Nada é publicado ou confirmado antes do commit.

## 2. Pacotes e regra de dependência

```
cmd ──▶ bootstrap (Fx) ──▶ httpapi, messaging, worker, auth, postgres, observability
                                   │            │          │
                                   ▼            ▼          ▼
                                  app (casos de uso + ports) ◀─ postgres implementa ports
                                   │
                                   ▼
                                domain, domain/money   (apenas std lib + google/uuid)
```

- `internal/domain` não conhece Fx, HTTP, SQS ou pgx. As entidades mantêm
  seu estado em campos não exportados e mudam apenas por meio de métodos
  validados. Construtores (`OpenWallet`, `NewExternalTransaction`,
  `NewLedgerEntry`) são separados da reidratação (`RehydrateWallet`,
  `RehydrateTransaction`), que valida mas nunca repete movimentos, transições
  ou eventos.
- `domain.Settle` é uma função pura que contém todas as regras de negócio dos
  cinco tipos. Ela muta a transação e a carteira em memória e retorna a entrada
  do ledger e os eventos. Todo o conjunto de regras é testado unitariamente sem
  banco de dados.
- `internal/app` declara as ports (`UnitOfWork`, repositórios, outbox store,
  publisher). HTTP e SQS chamam o mesmo `WageringService.Submit`.
- Erros de domínio podem ser classificados com `errors.Is`/`errors.As`:
  `ErrInvalidArgument`, `ErrInvalidTransition` (via `*TransitionError`),
  `ErrInvariantViolation` e `*RejectionError{Code}`. Rejeições de negócio
  nunca provocam `panic`. Toda função de I/O recebe um `context.Context`.

## 3. Dinheiro

- **Representação**: `money.Money{units int64, currency Currency}`. Os valores
  são contados em unidades menores (centavos) com escala fixa de 2. São aceitas
  apenas moedas ISO 4217 com 2 dígitos menores (`BRL`, `USD`, `EUR`, …). O
  valor zero é inválido e toda operação o rejeita com `ErrUninitialized`.
- **Limites**: de −92.233.720.368.547.758,08 a 92.233.720.368.547.758,07.
  Parsing, `Add`, `Sub` e `Neg` detectam overflow e retornam `ErrOverflow`.
  `Neg(MinInt64)` é coberto.
- **Parsing** é feito dígito a dígito na string, sem `float` em nenhum lugar.
  A gramática é `-?(0|[1-9][0-9]*)(\.[0-9]{1,2})?`. Rejeita strings vazias,
  `NaN`, `Infinity`, notação científica, `+` no início, zeros à esquerda, `.`
  no final e mais de 2 casas decimais (`ErrScaleExceeded`: nunca arredonda).
  Entradas externas usam `ParseNonNegative`, que rejeita negativos. Códigos de
  moeda devem estar em maiúsculas.
- **Formas equivalentes aceitas**: `"25"`, `"25.5"` e `"25.50"`. Antes de
  calcular o hash e persistir, são normalizadas para exatamente dois decimais
  (`"25.00"`, `"25.50"`).
- **Serialização**: sempre `{"amount":"25.00","currency":"BRL"}`. O valor é
  uma *string* JSON, e um número JSON é recusado no momento da decodificação.
- **Persistência**: unidades menores `BIGINT` mais moeda `CHAR(3)` em todas
  as tabelas (`balance_minor`, `amount_minor`, `balance_before_minor`, …). O
  mapeamento passa por `money.FromUnits`. Agregações (reconciliação) somam em
  `NUMERIC` e fazem cast de volta para `BIGINT`, de modo que um overflow gera
  erro em vez de transbordar.
- Aritmética e comparação requerem a mesma moeda (`ErrCurrencyMismatch`).
  Valores negativos são permitidos para diferenças internas (ex.: `difference`
  na reconciliação), nunca para saldos.

## 4. Persistência e limites de transação

- **Biblioteca**: `pgx/v5` (`pgxpool`) com SQL explícito, sem ORM. Locks
  (`FOR UPDATE`, `SKIP LOCKED`), atualizações compare-and-set e nomes de
  constraints são todos visíveis em `internal/postgres`.
- **Unit of work**: `UnitOfWork.Run(ctx, fn)` abre uma transação `READ
  COMMITTED`. Todo repositório acessado pelo argumento `Tx` (`Wallets()`,
  `Transactions()`, `Ledger()`, `Outbox()`, `Inbox()`) compartilha-a, de modo
  que suas gravações fazem commit ou rollback juntas. `UnitOfWork.Snapshot`
  abre uma transação `REPEATABLE READ READ ONLY`, usada pela reconciliação.
- **Classificação de erros** (`postgres.mapErr`):

  | Erro | Classe | Tratamento |
  | --- | --- | --- |
  | violação de unicidade, deadlock, falha de serialização | `ErrRetryableConflict` | o caso de uso repete o unit inteiro até 5 vezes com jitter; o retry então observa o vencedor e vira um replay |
  | miss de version CAS | `ErrConcurrentUpdate` | igual ao anterior |
  | timeout de lock (`55P03`), cancelamento, shutdown administrativo, erros de conexão, resultado do commit desconhecido | `ErrTransient` | HTTP 503 com `Retry-After`; backoff de visibilidade no SQS |
  | violações de check, exceções de trigger, qualquer outra coisa | permanente | HTTP 500 / SQS retenta até redrive para a DLQ / worker marca `FAILED` |

- **Tempo**: o relógio da aplicação (UTC) marca os fatos de domínio. Os leases
  do outbox usam o `now()` do banco, de modo que a diferença de relógio entre
  instâncias não pode quebrá-los.
- **Migrações**: arquivos SQL goose com seções `Up`/`Down`, embutidos e
  executados pelo `cmd/migrate` com o papel de proprietário. O papel de
  runtime `wallet_app` só recebe `SELECT, INSERT` no ledger e no inbox
  (sem `UPDATE`/`DELETE`).

## 5. Invariantes aplicados pelo banco de dados

O esquema aplica os invariantes por conta própria, independentemente dos locks
da aplicação e da deduplicação FIFO do SQS:

| Invariante | Mecanismo |
| --- | --- |
| Saldo não negativo | `CHECK (balance_minor >= 0)`; saldos do ledger também `>= 0` |
| Uma carteira por (jogador, moeda) | `UNIQUE (player_id, currency)` |
| Toda mudança de saldo tem sua entrada no ledger no mesmo commit | trigger de constraint `DEFERRABLE INITIALLY DEFERRED` `wallets_ledger_consistency`: no commit, cada mudança de saldo deve ter uma entrada com o mesmo `wallet_version`, `balance_before` e `balance_after` |
| A versão começa em 1 e cresce exatamente 1 por mudança de saldo | trigger `wallets_guard` |
| Sem lost update / sem histórico bifurcado | `UNIQUE (wallet_id, wallet_version)` no ledger + version CAS + row lock |
| Aritmética do ledger | `CHECK (after = before ± amount)` por direção |
| Uma entrada do ledger por transação | `UNIQUE (wallet_id, transaction_id)` |
| Ledger é append-only | triggers `BEFORE UPDATE OR DELETE` e `BEFORE TRUNCATE` + sem privilégios para o papel de runtime |
| Idempotência | `UNIQUE (provider_id, external_transaction_id)` e `UNIQUE (provider_id, idempotency_key)` para transações externas |
| Sem crédito de abertura duplicado | `UNIQUE (wallet_id) WHERE kind='OPENING'` parcial |
| No máximo uma reversão bem-sucedida por referência | `UNIQUE (reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'` parcial |
| Forma interna vs externa | `CHECK` em `origin`: OPENING não tem provider/external id/key/hash/round/game/reference; linhas externas os exigem |
| Política de zero | `CHECK ((kind='LOSS' AND amount=0) OR (kind<>'LOSS' AND amount>0))` |
| Transações terminais são finais; identidade/payload imutáveis; sem delete | trigger `wager_transactions_guard` |
| Snapshot do outbox imutável; publicado permanece publicado | trigger `outbox_events_guard` |
| Inbox é imutável; uma linha por (consumer, message) | trigger + `PRIMARY KEY (consumer_name, message_id)` |

`test/integration/db_test.go` exercita cada um desses com SQL puro, sem passar
pela aplicação.

## 6. Controle de concorrência

**Estratégia: lock pessimista por linha de carteira, mais compare-and-set
otimista de versão, mais constraints do banco de dados.**

1. `SELECT … FROM wallets WHERE id = $1 FOR UPDATE` serializa os escritores de
   uma carteira. Outras carteiras não são afetadas, e não há lock global.
   `TestIndependentWalletsProgressInParallel` mantém o lock de uma carteira
   enquanto 30 outras carteiras continuam processando.
2. `UPDATE wallets SET … WHERE id = $1 AND version = $expected` é um segundo
   guarda independente contra lost updates.
3. `UNIQUE (wallet_id, wallet_version)` e o trigger de consistência diferido
   são a última linha de defesa, mesmo que ambos os guardas da aplicação
   tivessem um bug.

Por que não `SERIALIZABLE` ou locking puramente otimista? Uma carteira quente
sob locking otimista produz tempestades de retry. `SERIALIZABLE` aborta
transações em conflitos de predicado não relacionados à carteira. Um row lock
oferece enfileiramento determinístico por carteira, e `lock_timeout` (5s)
limita a espera, aparecendo como um 503/retry transitório.

**Duplicatas sob concorrência**: uma consulta de idempotência rápida roda sem
o lock. Se nada for encontrado, a carteira é bloqueada e a consulta é repetida.
Duplicatas concorrentes visam a mesma carteira, então enfileiram nesse lock e
veem a linha commitada. Uma duplicata com um `walletId` diferente perde no
índice único, faz retry e vira um replay ou um conflito.

**Ordem de lock** (prevenção de deadlock):

- `Submit` faz: lookup de inbox → lock de carteira.
- O worker de pendentes faz o lock da carteira com `SKIP LOCKED`, depois lê a
  transação.

Nada bloqueia uma linha de transação antes de uma carteira.

**Cenário obrigatório** (100,00 BRL, duas apostas concorrentes de 80,00): um
`PROCESSED`, um `REJECTED/INSUFFICIENT_FUNDS`, saldo final de 20,00, um único
débito. Reenvios não mudam nada. É verificado 10 vezes em processo com 3 pools
independentes, e 10 vezes nos três processos do Compose.

## 7. Idempotência

- **Chave**: o header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS)
  é obrigatório e armazenado exatamente como recebido. O servidor nunca
  substitui por uma chave calculada. As chaves são escopadas por provedor:
  `UNIQUE (provider_id, idempotency_key)`.
- **Hash do payload**: SHA-256 (hex) sobre um JSON canônico com chaves
  ordenadas lexicograficamente e sem espaços. Os campos são
  `externalTransactionId`, `gameId`, `kind`, `money{amount,currency}`,
  `playerId`, `providerId`, `referenceExternalTransactionId` (apenas quando
  presente), `roundId` e `walletId`. Normalizações: UUIDs em forma canônica
  minúscula, valor com exatamente dois decimais, moeda em maiúscula ISO. 
  **Excluídos**: a chave de idempotência, `messageId`, correlation ids,
  timestamps e headers. HTTP e SQS constroem o mesmo `RawExternalRequest`,
  portanto uma operação obtém o mesmo hash por ambos os pontos de entrada
  (testado unitariamente em `messaging/message_test.go`).
- **Regras** (`WageringService.findExisting`):

  | Situação | Resultado |
  | --- | --- |
  | mesma chave, mesmo hash | replay: resultado persistido com `idempotentReplay: true` |
  | mesma chave, hash diferente | `409 IDEMPOTENCY_KEY_CONFLICT` |
  | mesmo `(providerId, externalTransactionId)` com outra chave | `409 EXTERNAL_TRANSACTION_CONFLICT` (nunca reaplicado) |

- **Resultado do replay**: `result_balance_minor` armazena o saldo observado
  quando a operação foi concluída (ou rejeitada), de modo que um replay retorna
  o saldo original mesmo após movimentações posteriores. Um replay de uma
  transação pendente retorna `202` com seu status atual.
- **Durabilidade**: tudo isso vive no PostgreSQL e sobrevive a reinicializações
  de todos os processos (`TestIdempotencyRules` usa um pool novo; o teste
  de chaos e2e encerra um processo).

## 8. Máquina de estados da transação e classificação de falhas

```
PENDING ──────────────┬──▶ PROCESSED  (terminal)
   │                  ├──▶ REJECTED   (terminal, failureCode)
   ▼                  └──▶ FAILED     (terminal, failureCode)
PENDING_REFERENCE ────┘
   ▲  │  reagendamento (attempts++, nextAttemptAt avança)
   └──┘
```

- As transições são métodos (`MarkPendingReference`, `MarkProcessed`,
  `MarkRejected`, `MarkFailed`). A partir de um estado terminal, cada um
  retorna `*TransitionError` (`errors.Is(err, ErrInvalidTransition)`). O
  trigger do banco também as recusa.
- **Caminho síncrono**: operações sem dependência pendente vão de `PENDING`
  para um estado terminal em memória, dentro da mesma transação, sem commit
  intermediário. Uma linha `PENDING` portanto nunca é commitada pelos caminhos
  normais. O worker, no entanto, retoma qualquer linha `PENDING` commitada
  (por exemplo, uma deixada por um caminho assíncrono futuro que travou), como
  `TestResumeCrashedPending` demonstra.
- **Transitório vs permanente**:
  - **Transitório**: indisponibilidade de banco ou broker, timeout de lock,
    deadline, corridas de concorrência. Esses são retentados: um retry
    imediato limitado para corridas, depois 503 via HTTP, backoff de
    visibilidade no SQS, ou o próximo poll do worker. Nenhum estado é gravado.
  - **Resultado de negócio permanente**: `REJECTED` com um `failureCode`;
    persistido e terminal.
  - **Falha de infraestrutura permanente**: ao retomar uma transação persistida,
    um erro não transitório como uma constraint violada ou dados corrompidos.
    A transação é marcada `FAILED` com `PROCESSING_FAILED` para auditoria, e
    nenhum evento é emitido.
  - **Entrada inválida**: nunca persistida. HTTP responde 400/404; SQS envia a
    mensagem para a DLQ.

## 9. Operações, referências e reversões

| Tipo | Movimento | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0; saldo ≥ valor, caso contrário `INSUFFICIENT_FUNDS` |
| `WIN` | crédito | valor > 0; referência opcional deve ser um `BET PROCESSED` da mesma rodada |
| `LOSS` | nenhum | valor deve ser `0.00`; moeda da carteira obrigatória; sem entrada no ledger, sem mudança de versão; emite apenas `WagerTransactionProcessed` |
| `REFUND` | crédito | referência obrigatória; deve ser um `BET PROCESSED`; mesmo valor |
| `ROLLBACK` | oposto da referência | referência obrigatória; `BET` → crédito, `WIN`/`REFUND` → débito; mesmo valor; um débito além do saldo é `REVERSAL_INSUFFICIENT_FUNDS` (distinto de `INSUFFICIENT_FUNDS`, auditável) |
| `OPENING` | crédito | somente interno; rejeitado quando recebido via HTTP ou SQS |

- Uma referência é resolvida por `(providerId, referenceExternalTransactionId)`.
  Provedor, jogador, carteira, moeda e rodada devem coincidir
  (`REFERENCE_MISMATCH`). Reversões parciais são recusadas
  (`REVERSAL_AMOUNT_MISMATCH`).
- **Combinando `REFUND` e `ROLLBACK` na mesma aposta**: uma transação
  referenciada aceita **no máximo uma reversão bem-sucedida no total**,
  independentemente do tipo. Isso é mais estrito que "uma por tipo". Impede
  devolver o mesmo débito duas vezes, por exemplo com um `REFUND` seguido de
  um `ROLLBACK` do mesmo `BET`. O índice único parcial aplica isso sob
  concorrência (`TestConcurrentReversalsOfSameBet`). Um `ROLLBACK` de um
  `REFUND` é permitido: desfaz o reembolso e redebita. Após isso, o `BET`
  original não pode ser reembolsado novamente, o que é conservador e nunca
  credita a mais. Um `ROLLBACK` de um `ROLLBACK` é recusado
  (`REFERENCE_KIND_NOT_ALLOWED`).
- **Política de zero**: zero é aceito apenas para o saldo inicial e para
  `LOSS`. O domínio, a validação da requisição e uma constraint `CHECK` a
  aplicam.

### Referências ainda não disponíveis

- Uma referência ausente torna a operação `PENDING_REFERENCE`. A operação é
  commitada, emite `WagerTransactionPendingReference` e recebe `nextAttemptAt`
  e `referenceExpiresAt = createdAt + TTL`. O cliente recebe `202`.
- O **worker de referências pendentes** roda em todas as instâncias e sobrevive
  a reinicializações, pois seu estado vive na tabela. Ele lista transações
  devidas e, para cada uma, bloqueia a carteira com `FOR UPDATE SKIP LOCKED`
  (carteiras ocupadas em outro lugar são ignoradas até o próximo poll). Depois
  relê a transação, verifica se ainda está aberta e devida, e executa `Settle`
  novamente.
- **Backoff**: exponencial. Começa em 500ms e dobra até um limite de 1 minuto
  (`PENDING_BASE_BACKOFF`, `PENDING_MAX_BACKOFF`), com polling com jitter.
  `attempts` é persistido, então o orçamento sobrevive a reinicializações.
- **Expiração**: após `PENDING_MAX_ATTEMPTS` (10, contando a primeira tentativa
  síncrona) ou `PENDING_TTL` (10 minutos), a operação vira `REJECTED` com
  `REFERENCE_NOT_FOUND`, e `WagerTransactionRejected` é emitido.
- **Referência existe mas ainda está pendente** (`PENDING`/`PENDING_REFERENCE`):
  a operação espera com o mesmo backoff e orçamento, e expira com
  `REFERENCE_NOT_PROCESSED`.
- **Referência terminou sem sucesso** (`REJECTED`/`FAILED`): a operação é
  imediatamente `REJECTED` com `REFERENCE_NOT_PROCESSED`.

## 10. Consumidor SQS e inbox

- **Identidade durável da mensagem**: o `messageId` do envelope. A chave do
  inbox é `(consumer_name, message_id)`, e o hash do inbox é
  `SHA-256(type + "\n" + idempotencyKey + "\n" + payloadHash)`.
- **Atomicidade**: o registro do inbox é inserido na **mesma transação SQL**
  que a linha de transação, o saldo, a entrada do ledger e os eventos do
  outbox. Para uma referência pendente, a linha do inbox é commitada junto com
  o registro `PENDING_REFERENCE`, e o worker assume a partir daí.
- **Reentrega**: um `messageId` conhecido com o mesmo hash é confirmado e
  retorna o resultado armazenado. Um hash diferente gera `ErrMessageConflict`
  e a mensagem vai para a DLQ. Um novo `messageId` para uma operação já
  registrada é capturado pelas regras de idempotência.
- **Deleção somente após o commit**. `TestCrashAfterCommitBeforeDelete` faz o
  consumidor "morrer" entre o commit e o `DeleteMessage`. A mensagem retorna
  (receive count 2) para um consumidor independente, que a confirma pelo inbox
  com um único débito.
- **Resultados**:

  | Resultado | Quando | Ação no broker |
  | --- | --- | --- |
  | ack | processado, rejeição de negócio, referência pendente, duplicata | `DeleteMessage` |
  | retry | erro transitório ou desconhecido | `ChangeMessageVisibility` para `min(2s·2^(receiveCount−1), 60s)` |
  | DLQ | JSON/envelope inválido, tipo/campos desconhecidos, `OPENING`, erro de validação, provedor não em `SQS_ALLOWED_PROVIDERS`, carteira não encontrada, conflitos de idempotência/mensagem | copia para `wager-transactions-dlq.fifo` com atributos `failureReason`/`failureDetail`, depois deleta |
  | release | shutdown interrompeu o handling | visibilidade → 0, para outra instância assumir imediatamente |

- **Limites**: timeout de visibilidade 30s; timeout do handler 20s (validado
  para ser menor que o de visibilidade); **`maxReceiveCount` = 5**, após o qual
  a política de redrive move a mensagem para a DLQ; até 10 mensagens por
  receive; long polling de 10s.
- **Ids de grupo e deduplicação**:
  - Os produtores devem enviar `MessageGroupId = walletId`. Isso ordena as
    mensagens de cada carteira e permite que carteiras diferentes processem em
    paralelo (a fila usa `DeduplicationScope=messageGroup` e
    `FifoThroughputLimit=perMessageGroupId`).
  - `MessageDeduplicationId = messageId`. A dedup de 5 minutos do broker é
    apenas uma otimização; o inbox é a deduplicação durável.
  - Dentro de um lote recebido, as mensagens de um grupo são tratadas
    sequencialmente e grupos diferentes de forma concorrente.
  - Chegada fora de ordem (uma reversão antes de sua aposta) é tratada pelo
    domínio via `PENDING_REFERENCE`, sem depender da ordenação FIFO.
- **Concorrência HTTP vs SQS**: ambos os caminhos terminam em `Submit` e
  serializam no lock da carteira. O perdedor vira um replay
  (`TestSameOperationThroughHTTPAndSQS`, `TestE2EHTTPAndSQSSameOperation`,
  que também envia a mesma mensagem 3 vezes com ids de dedup diferentes do
  broker para forçar a dedup da aplicação).
- **SIGTERM**:
  1. O polling para imediatamente.
  2. Os handlers em andamento terminam dentro do prazo de parada.
  3. Se o prazo expirar, os handlers são cancelados (suas transações fazem
     rollback) e suas mensagens são liberadas com visibilidade 0.

  Chamadas de bookkeeping do broker (delete, mudanças de visibilidade) usam um
  contexto desanexado do shutdown.

## 11. Outbox transacional

- Os eventos são linhas em `outbox_events`, gravadas na mesma transação que
  sua causa. O relay só pode ver linhas commitadas, então **nada é publicado
  antes do commit**.
- **Relay**: roda em todas as instâncias. Reclama lotes com
  `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, definindo
  `locked_by`, `locked_until = now() + lease` e `attempts++`. Publishers
  concorrentes nunca reclamam a mesma linha ao mesmo tempo.
  - Falhas: `next_attempt_at = now + min(1s·2^(attempts−1), 5m)` e o lease é
    liberado.
  - Trabalho abandonado: quando uma instância morre enquanto mantém um lease,
    o lease expira (30s) e outra instância reclama as linhas.
- **Janelas de crash**:
  - *Após commit, antes de publicar*: a linha permanece não publicada e é
    reclamada depois.
  - *Após publicar, antes da confirmação*: o lease expira e a linha é
    republicada **com o mesmo `eventId`**. O id vem do snapshot imutável
    armazenado e é usado como `MessageDeduplicationId` do FIFO.
  - `TestOutboxCompetingPublishersAndRecovery` simula a segunda janela com dois
    relays competindo e verifica que cada evento é publicado, que os eventos
    crashados foram entregues com seus ids originais, e que `attempts ≥ 2`.
- **Semântica de entrega**: at-least-once. Os consumidores devem deduplicar
  por `eventId`.
- **Contrato de roteamento** (`wallet-events.fifo`):
  `MessageGroupId = aggregateId` (o id da carteira para
  `WalletBalanceChanged`, o id da transação para os eventos de transação),
  `MessageDeduplicationId = eventId`, e atributos de mensagem `eventType` e
  `eventId`. A ordenação entre agregados e entre instâncias após um retry não
  é garantida. Consumidores de eventos de saldo devem ordenar por
  `walletVersion`.
- **Envelope** (construído por construtores tipados que fixam `eventType` e
  `version`; timestamps UTC RFC 3339; money como strings decimais; o payload
  armazenado é um snapshot imutável):

```json
{
  "eventId": "01a0…", "eventType": "WalletBalanceChanged", "aggregateType": "Wallet",
  "aggregateId": "<walletId>", "correlationId": "<requisição ou correlação da mensagem>",
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

| Evento | Agregado | Gatilho | Dados (além dos campos comuns de transação) |
| --- | --- | --- | --- |
| `WagerTransactionProcessed` | transação | sucesso, incluindo `LOSS` e `OPENING` | `balanceAfter`, `processedAt` |
| `WagerTransactionRejected` | transação | rejeição definitiva de negócio | `failureCode`, `rejectedAt` |
| `WalletBalanceChanged` | carteira | mudança efetiva de saldo | como acima |
| `WagerTransactionPendingReference` | transação | primeira movimentação para `PENDING_REFERENCE` | `nextAttemptAt`, `referenceExpiresAt` |

Os campos comuns de transação são `transactionId`, `origin`, `kind`,
`status`, `walletId`, `playerId`, `money`, além de `providerId`,
`externalTransactionId`, `roundId`, `gameId`, `referenceExternalTransactionId`
e `referenceTransactionId` quando aplicáveis. Esses campos externos são
omitidos para o `OPENING` interno.

## 12. Autenticação e autorização

- **IdP: Keycloak** (no Compose; realm, clientes, papéis e mappers são
  importados de `deploy/keycloak/realm-wagering.json`). É um servidor padrão
  OAuth 2.0/OIDC com rotação de chaves JWKS e o grant `client_credentials`
  para chamadas serviço a serviço. Suporta mappers de *claim hard-coded*
  por cliente, que é como cada identidade de provedor é vinculada às suas
  credenciais. O serviço nunca armazena senhas e nunca emite tokens.
- **Validação de token** (`internal/auth`, `go-oidc`):
  - Assinatura RS256 contra o JWKS do realm (cacheado, atualizado em `kid`
    desconhecido);
  - `iss` igual ao issuer público; `aud` contendo `wagering-api` (um mapper
    de audience em cada cliente); `exp` verificado;
  - `typ` deve ser `Bearer`, o que rejeita tokens de ID e de refresh.

  No Compose o JWKS é buscado pelo hostname interno, enquanto `iss` é a URL
  pública (`KC_HOSTNAME`).
- **Modelo de permissões**: papéis do realm em `realm_access.roles`.
  - `wagering-provider`: a identidade do provedor é **apenas** o claim
    `provider_id`. O `providerId` do corpo deve igualá-lo (403 caso contrário),
    então um provedor não pode submeter ou fazer replay em nome de outro.
    Chaves de idempotência e ids externos são escopados por provedor no
    esquema. Ler a transação de outro provedor por id retorna 404 (a existência
    não é revelada); a rota escopada por provedor retorna 403.
  - `wallet-operator` (o serviço interno): endpoints de carteira, o ledger,
    reconciliação, leitura de qualquer transação. Não pode submeter operações
    de provedor.
  - Um token sem esses papéis recebe 403. Um token ausente, inválido,
    adulterado ou expirado recebe 401.
- Requisições rejeitadas nunca chegam a um caso de uso, portanto não causam
  efeito financeiro e não expõem dados. `TestAuthentication` e
  `TestAuthorizationAndProviderIsolation` verificam isso contra o Keycloak
  real, incluindo tokens de 2 segundos usados após expirarem.
- **Acesso ao messaging**:
  - O serviço se autentica no SQS com suas próprias credenciais.
  - As políticas da fila (provisionadas em `init-sqs.sh`) permitem apenas os
    principais dos provedores de `SendMessage` para a fila de entrada, e
    apenas o principal `wallet-service` de consumi-la, usar a DLQ e publicar
    eventos.
  - O consumidor ainda aplica toda validação de domínio e uma allow-list de
    provedores (`SQS_ALLOWED_PROVIDERS`).

## 13. Composição Uber Fx, ciclo de vida e encerramento

- **Módulos** (`internal/bootstrap`): `core` (validação de config, logger,
  métricas), `postgres`, `sqs`, `app`, `auth`, `http` e `workers`. Usam
  injeção por construtor com `fx.Provide`, e `fx.Annotate`/`fx.As` para
  vincular adaptadores a ports. Um único `fx.Invoke(registerLifecycle)` anexa
  os hooks dos pontos de entrada em uma ordem explícita.
- **Início**:
  1. A configuração é validada (valores obrigatórios, timeout do handler menor
     que o de visibilidade, …).
  2. O pool pinga o PostgreSQL com backoff até o timeout de conexão.
  3. As URLs das filas são resolvidas.
  4. Os workers iniciam.
  5. O servidor HTTP vincula sua porta de forma síncrona, então uma porta
     ocupada falha rapidamente.
- **Parada** (ordem inversa, limitada pelo `SHUTDOWN_TIMEOUT` = 25s;
  `stop_grace_period` do Compose é 30s):
  1. O servidor HTTP reporta not-ready (503 em `/health/ready`), para de
     aceitar conexões e drena requisições em andamento.
  2. O consumidor SQS para de fazer polling e termina ou libera mensagens
     em andamento.
  3. O loop do relay do outbox para. Um lote interrompido é recuperado pela
     expiração do lease, e eventos publicados são confirmados graças a um
     contexto de bookkeeping desanexado.
  4. O worker de pendentes para. Um unit of work interrompido faz rollback.
  5. O pool do PostgreSQL é fechado.

  As dependências fecham apenas após todo componente que as usa ter terminado.
  Isso foi verificado nos logs de `docker compose stop app2`.
- **Workers** (`worker.Loop`) possuem seu contexto, param por cancelamento e
  expõem `Done()` para terminação observável. `TestFxLifecycle` inicia e para
  todo o grafo com `fxtest` e verifica que os loops terminaram, que o pool
  recusa pings e que a porta está fechada.

## 14. Observabilidade

- **Logs**: JSON via `log/slog`, com atributos contextuais injetados por um
  handler (`observability.WithLogFields`): `correlationId`, `messageId`,
  `sqsMessageId`, `transactionId`, `walletId`, `providerId`, `instance`.
- **Métricas**: listadas no README. Cobrem resultados por status,
  duplicatas, retries, DLQ, conflitos de concorrência, lag do outbox, latência
  e divergências de reconciliação (que também são registradas no nível ERROR e
  retornadas na resposta).
- **Health checks**: liveness, mais readiness do PostgreSQL e SQS.
- Tracing (OpenTelemetry) e dashboards não foram implementados; veja §16.

## 15. Cenários de falha

| Cenário (§3 do desafio) | Tratamento | Evidência |
| --- | --- | --- |
| Mesma operação recebida repetidamente (HTTP e SQS) | inbox + chave única/id externo escopados por provedor + re-verificação com lock | `TestSameBetFiftyTimesInParallel`, `TestSameOperationThroughHTTPAndSQS`, equivalentes e2e |
| Reversão antes de sua referência | `PENDING_REFERENCE` + worker durável com backoff/TTL | `TestReversalBeforeReferenceIsResolvedLater`, `TestPendingReferenceExpires` |
| Operações concorrentes em uma carteira | row lock + CAS + constraints | `TestTwoConcurrentBetsOnLimitedBalance`, `TestManyConcurrentMixedOperationsOnOneWallet` |
| Parada abrupta antes do commit | a transação faz rollback; o cliente/SQS reenviam com a mesma chave | `TestFinancialAtomicity`, `TestE2EChaosKillInstance` |
| Parada abrupta após o commit | replay por chave / inbox; a mensagem é reenviada e confirmada | `TestCrashAfterCommitBeforeDelete`, teste de chaos |
| Publicação repetida de um evento | `eventId` estável + dedup FIFO + dedup do consumidor | `TestOutboxCompetingPublishersAndRecovery` |
| PostgreSQL ou SQS temporariamente indisponível | classificação transitória → 503 / backoff de visibilidade / redrive; início espera por dependências; readiness falha | `TestTransientFailuresExhaustToDLQ`, health checks |

## 16. Interpretações, limitações e trabalho não concluído

- **Aplicação de IAM no broker**: o LocalStack Community armazena políticas de
  fila mas não aplica IAM. As políticas são provisionadas como seriam na AWS,
  mas localmente qualquer credencial pode chamar o SQS. As validações de
  domínio e a allow-list de provedores ainda se aplicam no consumidor. As
  mensagens SQS não carregam uma identidade de provedor verificada por
  mensagem. Na AWS, uma fila por provedor, ou condições de `aws:SourceArn`/
  principal, vinculariam a identidade ao canal.
- **Rejeitado vs não persistido**: operações cuja carteira não existe, e
  requisições que falham na validação, não são persistidas (HTTP 400/404, DLQ
  do SQS). Apenas rejeições de operações bem formadas em uma carteira existente
  viram linhas `REJECTED`.
- **`FAILED`** é produzido apenas pelo worker de pendentes em erros não
  transitórios. Erros permanentes inesperados nos caminhos síncronos não são
  persistidos: HTTP retorna 500, e o SQS reenviarà até a mensagem ser redirecionada
  para a DLQ.
- **Política de reversão** é mais estrita que o exigido: uma reversão bem-
  sucedida por referência no total (veja §9).
- **`WIN` com referência** espera pela referência como as reversões fazem
  (`PENDING_REFERENCE`). Sem referência é processado imediatamente.
- **O saldo da rejeição** retornado ao provedor é o saldo observado quando a
  operação foi rejeitada.
- **`POST /wallets`** não tem chave de idempotência: uma abertura repetida para
  o mesmo jogador e moeda retorna `409`, como o desafio especifica.
- **Ordenação do outbox** é best-effort (veja §11).
- **Ledger de dupla entrada, tracing OpenTelemetry, dashboards e testes de
  carga** (todos opcionais) não foram implementados.
- O requisito de "três processos independentes" é coberto pela suite e2e
  contra os containers `app1..app3` do Compose. A suite de integração também
  usa três pools e instâncias de serviço independentes em um único processo de
  teste, por velocidade e para rodar com `-race`.
