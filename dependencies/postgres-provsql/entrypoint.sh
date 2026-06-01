#!/bin/bash
set -e

# Start postgres via the official entrypoint in the background so we can
# run post-start SQL before handing control back.
docker-entrypoint.sh "$@" &
PG_PID=$!

# Forward SIGTERM/SIGINT to postgres so `docker stop` works cleanly.
trap 'kill -TERM $PG_PID' TERM INT

# Wait until postgres is accepting connections.
PGUSER="${POSTGRES_USER:-postgres}"
until pg_isready -U "$PGUSER" -q 2>/dev/null; do
    sleep 1
done

# Upgrade provsql in every non-template database that has it installed.
# This is a no-op when the extension is already at the current version.
for db in $(psql -U "$PGUSER" -Atc \
    "SELECT datname FROM pg_database WHERE datistemplate = false"); do
    psql -U "$PGUSER" -d "$db" -c "
        DO \$\$ BEGIN
            IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'provsql') THEN
                ALTER EXTENSION provsql UPDATE;
            END IF;
        END \$\$;" 2>/dev/null || true
done

wait "$PG_PID"
