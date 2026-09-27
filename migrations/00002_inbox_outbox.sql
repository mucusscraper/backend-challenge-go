-- Inbox (consumer-side deduplication) and transactional outbox.

-- +goose Up

CREATE TABLE inbox_messages (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    -- SHA-256 of the canonical message content; a redelivery with the same
    -- messageId but a different hash is refused.
    payload_hash   TEXT        NOT NULL,
    transaction_id UUID        REFERENCES wager_transactions (id),
    outcome        TEXT        NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    -- The eventId is generated once and is stable across republications.
    id              UUID        PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL CHECK (event_version >= 1),
    -- Full envelope snapshot, stored as JSON (exact text preserved).
    payload         JSON        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Lease: a publisher owns a row until locked_until; after that any other
    -- instance may claim it again (recovery of abandoned work).
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX outbox_events_due ON outbox_events (next_attempt_at) WHERE published_at IS NULL;
CREATE INDEX outbox_events_unpublished_occurred ON outbox_events (occurred_at) WHERE published_at IS NULL;

-- +goose StatementBegin
-- The event snapshot is immutable; only delivery bookkeeping may change and
-- a published event cannot be "unpublished".
CREATE FUNCTION guard_outbox_row() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox events cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.aggregate_type <> OLD.aggregate_type OR NEW.aggregate_id <> OLD.aggregate_id
        OR NEW.event_type <> OLD.event_type OR NEW.event_version <> OLD.event_version
        OR NEW.payload::text <> OLD.payload::text OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event snapshot is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'published_at cannot change once set' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER outbox_events_guard
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION guard_outbox_row();

-- +goose StatementBegin
-- Inbox rows are immutable records of completed handling.
CREATE FUNCTION forbid_inbox_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'inbox_messages is append-only (% not allowed)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER inbox_messages_no_update_delete
    BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION forbid_inbox_mutation();

-- Least privilege for the runtime role (created by deploy/postgres/init).
-- The application can never UPDATE/DELETE the ledger or inbox even if a bug
-- tried to, in addition to the triggers above.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        GRANT SELECT, INSERT, UPDATE ON wallets TO wallet_app;
        GRANT SELECT, INSERT, UPDATE ON wager_transactions TO wallet_app;
        GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;
        GRANT SELECT, INSERT ON inbox_messages TO wallet_app;
        GRANT SELECT, INSERT, UPDATE ON outbox_events TO wallet_app;
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        REVOKE ALL ON wallets, wager_transactions, wallet_ledger_entries FROM wallet_app;
    END IF;
END
$$;
-- +goose StatementEnd
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP FUNCTION IF EXISTS guard_outbox_row();
DROP FUNCTION IF EXISTS forbid_inbox_mutation();
