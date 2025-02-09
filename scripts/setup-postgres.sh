#!/usr/bin/env bash

export PGDATA=tmp/postgres-13
export PGHOST=127.0.0.1
export PGDATABASE=wal_development
export PGUSER=wal_development
export PGPASSWORD=wal_development
export PGURI="postgres://$PGUSER:$PGPASSWORD@$PGHOST"
export PGOPTIONS=" -c timezone=UTC -c synchronous_commit=off -c client_min_messages=warning"

wait_for_postgres() {
    until psql --no-psqlrc "$PGURI/postgres" -c '\q' 2>/dev/null; do
        sleep 0.2
    done
}

database_exists() {
    if [[ "$(psql --no-psqlrc "$PGURI/postgres" -qtAc "SELECT 1 FROM pg_database WHERE datname = '$1'")" == "1" ]]; then
        return 0
    else
        return 1
    fi
}

create_database() {
    psql --no-psqlrc "$PGURI/postgres" -c "CREATE DATABASE \"$1\";"
}

wait_for_postgres

if ! database_exists "$PGDATABASE"; then
    echo "== Creating postgres database =="
    create_database "$PGDATABASE"
fi
