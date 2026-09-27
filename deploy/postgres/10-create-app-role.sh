#!/bin/bash
# Creates the restricted runtime role used by the service. Migrations run as
# the database owner and GRANT only what the application needs (see
# migrations/00002_inbox_outbox.sql): the runtime role can never UPDATE or
# DELETE ledger or inbox rows.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
            CREATE ROLE wallet_app LOGIN PASSWORD '${APP_DB_PASSWORD:-wallet_app}';
        END IF;
    END
    \$\$;
    GRANT CONNECT ON DATABASE "$POSTGRES_DB" TO wallet_app;
    GRANT USAGE ON SCHEMA public TO wallet_app;
EOSQL
