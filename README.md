# WAL-Listener

Inspired after watching https://github.com/hasura/pgdeltastream

The service allows you to subscribe to changes in the PostgreSQL database using its logical decoding capability 
and publish them to the NATS Streaming server.

### Event Publishing

NATS Streaming is used as a message broker.
Service publishes the following structure.
The name of the topic for subscription to receive messages is formed from the prefix of the topic, 
the name of the database and the name of the table `prefix_dbname_tablename`.

```
{
	TableName string
	Action    string
	Data      map[string]interface{}
}
```

Messages are published to Nats-Streaming at least once!

## Restrictions

* DB Postgres must be configured for logical replication and wal2json extension installed.
(use for test `docker run -it -p 5432:5432 debezium/postgres:11`)
* Tables must have a primary key.
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