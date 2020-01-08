# WAL-Listener

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/ihippik/wal-listener)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/ihippik/wal-listener)
[![Build Status](https://travis-ci.com/ihippik/wal-listener.svg?branch=master)](https://travis-ci.com/ihippik/wal-listener)
![Codecov](https://img.shields.io/codecov/c/github/ihippik/wal-listener)

A service that helps implement the **Event-driven architecture**.

To maintain the consistency of data in the system, we will use **transactional messaging** - 
publishing events in a single transaction with a domain model change.

The service allows you to subscribe to changes in the PostgreSQL database using its logical decoding capability 
and publish them to the NATS Streaming server.

Inspired after watching https://github.com/hasura/pgdeltastream

### Event publishing

NATS Streaming is used as a message broker.
Service publishes the following structure.
The name of the topic for subscription to receive messages is formed from the prefix of the topic, 
the name of the database and the name of the table `prefix + schema_table`.

```
{
	Schema    string
	Table     string
	Action    string
	Data      map[string]interface{}
}
```

Messages are published to Nats-Streaming at least once!

## Restrictions

* DB Postgres must be configured for logical replication and `wal2json` extension installed  
(use for test `docker run -it -p 5432:5432 debezium/postgres:11`)
* Tables must have a primary key
* DDL, truncate and sequences are not replicated

### Filter configuration example

```yaml
databases:
  filter:
    tables:
      users:
        - insert
        - update

```
This filter means that we only process events occurring with the `users` table, 
and in particular `insert` and `update` data.