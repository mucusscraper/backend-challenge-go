-- Core financial schema: wallets, wager transactions and the append-only
-- ledger. Every financial invariant is enforced here, independently of the
-- application code (see ARCHITECTURE.md, "Invariants in the database").

-- +goose Up

-- Wallets -------------------------------------------------------------------
CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    -- Money is stored as BIGINT minor units (cents), never as float.
    balance_minor BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT      NOT NULL CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    -- (playerId, currency) identifies a single wallet.
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

-- Wager transactions --------------------------------------------------------
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

    -- Internal (OPENING) and external operations are distinguished by the
    -- schema: OPENING has no provider metadata, external ops require it.
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
    -- Zero-amount policy: LOSS is exactly zero, money-moving kinds are > 0.
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

-- Idempotency: one operation per (provider, external id) and per
-- (provider, idempotency key). Keys are scoped by provider so providers are
-- isolated from each other.
CREATE UNIQUE INDEX wager_transactions_provider_external_key
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';

-- A wallet has at most one OPENING credit.
CREATE UNIQUE INDEX wager_transactions_single_opening
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- A reference can have at most one successful reversal (REFUND or ROLLBACK).
CREATE UNIQUE INDEX wager_transactions_single_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- Work queue of the pending-reference worker.
CREATE INDEX wager_transactions_due
    ON wager_transactions (next_attempt_at) WHERE status IN ('PENDING', 'PENDING_REFERENCE');

CREATE INDEX wager_transactions_wallet ON wager_transactions (wallet_id, created_at);

-- Ledger --------------------------------------------------------------------
CREATE TABLE wallet_ledger_entries (
    -- seq gives a stable, gap-tolerant order for cursor pagination.
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
    -- One entry per (wallet, transaction): a transaction cannot move money twice.
    CONSTRAINT wallet_ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    -- One entry per wallet version: two writers cannot both apply on the same version.
    CONSTRAINT wallet_ledger_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

CREATE INDEX wallet_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

-- Protection triggers ---------------------------------------------------------

-- +goose StatementBegin
-- The ledger is append-only: UPDATE and DELETE always fail.
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
-- Wallet guard: identity is immutable; the version starts at 1 and grows by
-- exactly one per balance change, never otherwise. Wallets are never deleted.
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
-- Every balance change must be matched by a ledger entry committed in the
-- same transaction. The check is DEFERRED to COMMIT so the application may
-- write the wallet and the entry in any order.
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
-- Wager transactions: identity and payload are immutable, terminal states are
-- final, rows are never deleted.
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
