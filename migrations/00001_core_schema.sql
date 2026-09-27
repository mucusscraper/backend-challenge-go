-- Esquema financeiro central: carteiras, transações de apostas e o ledger
-- append-only. Cada invariante financeiro é imposto aqui, independentemente do
-- código da aplicação (veja ARCHITECTURE.md, "Invariantes no banco de dados").

-- +goose Up

-- Carteiras -----------------------------------------------------------------
CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- Dinheiro é armazenado como unidades menores BIGINT (centavos), nunca como float.
    balance_minor BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT      NOT NULL CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    -- (playerId, currency) identifica uma única carteira.
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- Transações de apostas -----------------------------------------------------
CREATE TABLE wager_transactions (
    id                                UUID        PRIMARY KEY,
    origin                            TEXT        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              TEXT        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            TEXT        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    amount_minor                      BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency                          CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID        REFERENCES wager_transactions (id),
    failure_code                      TEXT,
    result_balance_minor              BIGINT      CHECK (result_balance_minor >= 0),
    attempts                          INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   TIMESTAMPTZ,
    reference_expires_at              TIMESTAMPTZ,
    correlation_id                    TEXT,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    processed_at                      TIMESTAMPTZ,

    -- Operações internas (OPENING) e externas são distinguidas pelo
    -- schema: OPENING não tem metadados de provedor, ops externas os exigem.
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING' AND status = 'PROCESSED'
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL AND amount_minor > 0)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Política de zero: LOSS é exatamente zero, kinds que movem dinheiro são > 0.
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_transactions_reversal_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_terminal_shape CHECK (
        (status = 'PROCESSED' AND result_balance_minor IS NOT NULL AND failure_code IS NULL AND processed_at IS NOT NULL)
        OR (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL AND processed_at IS NOT NULL)
        OR (status = 'PENDING' AND failure_code IS NULL)
        OR (status = 'PENDING_REFERENCE' AND failure_code IS NULL AND next_attempt_at IS NOT NULL)
    )
);

-- Idempotência: uma operação por (provider, id externo) e por
-- (provider, chave de idempotência). As chaves têm escopo por provedor para
-- isolá-los uns dos outros.
CREATE UNIQUE INDEX wager_transactions_provider_external_key
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';

-- Uma carteira tem no máximo um crédito OPENING.
CREATE UNIQUE INDEX wager_transactions_single_opening
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- Uma referência pode ter no máximo uma reversão bem-sucedida (REFUND ou ROLLBACK).
CREATE UNIQUE INDEX wager_transactions_single_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- Fila de trabalho do worker de referências pendentes.
CREATE INDEX wager_transactions_due
    ON wager_transactions (next_attempt_at) WHERE status IN ('PENDING', 'PENDING_REFERENCE');

CREATE INDEX wager_transactions_wallet ON wager_transactions (wallet_id, created_at);

-- Ledger ---------------------------------------------------------------------
CREATE TABLE wallet_ledger_entries (
    -- seq fornece uma ordem estável e tolerante a lacunas para paginação por cursor.
    seq                  BIGINT      GENERATED ALWAYS AS IDENTITY UNIQUE,
    id                   UUID        PRIMARY KEY,
    wallet_id            UUID        NOT NULL REFERENCES wallets (id),
    transaction_id       UUID        NOT NULL REFERENCES wager_transactions (id),
    direction            TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency             CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor BIGINT      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  BIGINT      NOT NULL CHECK (balance_after_minor >= 0),
    wallet_version       BIGINT      NOT NULL CHECK (wallet_version >= 1),
    created_at           TIMESTAMPTZ NOT NULL,
    -- Uma entrada por (carteira, transação): uma transação não pode mover dinheiro duas vezes.
    CONSTRAINT wallet_ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    -- Uma entrada por versão de carteira: dois writers não podem aplicar na mesma versão.
    CONSTRAINT wallet_ledger_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

CREATE INDEX wallet_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

-- Triggers de proteção -------------------------------------------------------

-- +goose StatementBegin
-- O ledger é append-only: UPDATE e DELETE sempre falham.
CREATE FUNCTION forbid_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% not allowed)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER wallet_ledger_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();

CREATE TRIGGER wallet_ledger_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();

-- +goose StatementBegin
-- Guard de carteira: a identidade é imutável; a versão começa em 1 e cresce
-- exatamente um por mudança de saldo, nunca de outra forma. Carteiras nunca são deletadas.
CREATE FUNCTION guard_wallet_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 1 THEN
            RAISE EXCEPTION 'new wallets start at version 1' USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
        OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet version changes only with the balance' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet version must increase by exactly one per balance change'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER wallets_guard
    BEFORE INSERT OR UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION guard_wallet_row();

-- +goose StatementBegin
-- Cada mudança de saldo deve ser correspondida por uma entrada de ledger confirmada na
-- mesma transação. A verificação é DEFERIDA ao COMMIT para que a aplicação possa
-- escrever a carteira e a entrada em qualquer ordem.
CREATE FUNCTION check_wallet_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    before_minor BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance_minor = 0 THEN
            RETURN NULL;
        END IF;
        before_minor := 0;
    ELSE
        IF NEW.balance_minor = OLD.balance_minor THEN
            RETURN NULL;
        END IF;
        before_minor := OLD.balance_minor;
    END IF;
    PERFORM 1 FROM wallet_ledger_entries e
     WHERE e.wallet_id = NEW.id
       AND e.wallet_version = NEW.version
       AND e.balance_before_minor = before_minor
       AND e.balance_after_minor = NEW.balance_minor
       AND e.currency = NEW.currency;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'balance change of wallet % (version %) has no matching ledger entry', NEW.id, NEW.version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER wallets_ledger_consistency
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_wallet_ledger_consistency();

-- +goose StatementBegin
-- Transações de apostas: identidade e payload são imutáveis, estados terminais são
-- finais, linhas nunca são deletadas.
CREATE FUNCTION guard_wager_transaction_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager transactions cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
        OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
        OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
        OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
        OR NEW.round_id IS DISTINCT FROM OLD.round_id
        OR NEW.game_id IS DISTINCT FROM OLD.game_id
        OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
        OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wager transaction identity/payload is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION guard_wager_transaction_row();

-- +goose Down
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS forbid_ledger_mutation();
DROP FUNCTION IF EXISTS guard_wallet_row();
DROP FUNCTION IF EXISTS check_wallet_ledger_consistency();
DROP FUNCTION IF EXISTS guard_wager_transaction_row();
