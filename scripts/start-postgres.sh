#!/usr/bin/env bash

export PGDATA=tmp/postgres-13
export PGHOST=127.0.0.1
export PGDATABASE=wal_development
export PGUSER=wal_development
export PGPASSWORD=wal_development
export PGURI="postgres://$PGUSER:$PGPASSWORD@$PGHOST"
export PGOPTIONS=" -c timezone=UTC -c synchronous_commit=off -c client_min_messages=warning"

if [[ ! -d "$PGDATA" ]]; then
    echo "== Creating postgres database cluster =="
    initdb --username="$PGUSER" --pwfile=<(echo "$PGPASSWORD") --locale=C
fi

postgres -c unix_socket_directories= -c fsync=off -c full_page_writes=off -c max_connections=500 -c wal_level=logical
