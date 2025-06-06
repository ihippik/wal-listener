# WAL-Listener

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/ihippik/wal-listener)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/ihippik/wal-listener)
[![Publish Docker image](https://github.com/ihippik/wal-listener/actions/workflows/github-actions.yml/badge.svg)](https://github.com/ihippik/wal-listener/actions/workflows/github-actions.yml)

![WAL-Listener](wal-listener.png)

A service that publishes changes made to a Postgres database to a pub sub bus. Subscribes to postgres via postgres' built-in logical replication protocol, applies some filters, and then publishes to your bus of choice. 

Key characteristics: 
 - implements a reliable, end-to-end at-least-once publishing model, where events aren't marked as consumed until they are ack'd in the bus
 - implemented using modern postgres go libs for maximum protocol compatability, including TOAST and other long-tail postgres feature support

### Event publishing

You can use the following message brokers:

- NATS JetStream [`type=nats`];
- Apache Kafka [`type=kafka`];
- RabbitMQ [`type=rabbitmq`].
- Google Pub/Sub [`type=google_pubsub`].

The name of the topic for subscription to receive messages is formed from the prefix of the topic,
the name of the database and the name of the table `prefix + schema_table`.

```go
{
	ID        uuid.UUID       # unique ID
	Schema    string
	Table     string
	Action    string
	Data      map[string]any
	DataOld   map[string]any  # old data (see DB-settings note #1)
	EventTime time.Time       # commit time
}
```

Messages are published to the broker at least once!

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

### Topic mapping

By default, output topics name consist of prefix, DB schema, and DB table name, but if you want to send all update in one topic you should be configured the topic map:
```yaml
topicsMap:
  main_users: "notifier"
  main_customers: "notifier"
```

### Adding tags

You can also add tags to every published event using the `tags` configuration option:

```yaml
tags:
  database: "main"
```

## DB setting

You must make the following settings in the db configuration (postgresql.conf)
* wal_level >= "logical"
* max_replication_slots >= 1

The publication & slot created automatically when the service starts (for all tables and all actions).
You can delete the default publication and create your own (name: _wal-listener_) with the necessary filtering conditions, and then the filtering will occur at the database level and not at the application level.

https://www.postgresql.org/docs/current/sql-createpublication.html

If you change the publication, do not forget to change the slot name or delete the current one.

Notes:

1. To receive `DataOld` field you need to change REPLICA IDENTITY to FULL as described here:
   [#SQL-ALTERTABLE-REPLICA-IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)

## Example configuration

```yaml
listener:
  slotName: myslot_1
  refreshConnection: 30s
  heartbeatInterval: 10s
  filter:
    tables:
      seasons:
        - insert
        - update
  topicsMap:
    schema_table_name: "notifier"
  skipTransactionBuffering: false
  dropForeignOrigin: false
logger:
  level: info
  fmt: json
database:
  host: localhost
  port: 5432
  name: my_db
  user: postgres
  password: postgres
  debug: false
publisher:
   type: nats
   address: localhost:4222
   topic: "wal_listener"
   topicPrefix: ""
monitoring:
  sentryDSN: "dsn string"
  promAddr: ":2112"
```

### Configuration settings



#### `listener.skipTransactionBuffering`

By default, `wal-listener` reads messages from Postgres until a full transaction has been seen, and then emits it to the bus. This ensures the bus doesn't see any partial transactions, as well as compatability with the newer postgres logical replication protocols that stream transactions in chunks.

However, it means that `wal-listener` needs enough memory to buffer your database's biggest transactions to successfully publish. For databases with large transactions this can be untenable. To publish messages as soon as they are seen in the replication stream, which doesn't require all this memory, you can set `listener.skipTransactionBuffering: true`

```yaml
listener:
  skipTransactionBuffering: true
```

#### `listener.dropForeignOrigin`

By default, `wal-listener` will publish all messages that pass your filters from Postgres, including those that the database received from subscriptions to further upstream databases. 

If you'd like to ignore messages that originate on these upstream databases, set `listener.dropForeignOrigin: true`.

__NOTE__: `listener.dropForeignOrigin` only works on Postgres v16 or later, as Postgres only sends the required replication messages on this version.

Here's what the setup might look like:

```
                                                 
                  logical                        
                  replication                    
 ┌──────────────┐           ┌──────────────┐     
 │  Database A  │ ────────► │  Database B  │     
 └──────────────┘           └──────────────┘     
                                                 
                                   │ logical     
                                   │ replication 
                                   ▼             
                          ┌──────────────────┐   
                          │   wal-listener   │   
                          └──────────────────┘   
                                                 
```

In this example, by default, `wal-listener` will publish all transactions that happen on `Database B`, which will __include__ all those that originated on `Database A` and were replicated over to `Database B`. `listener.dropForeignOrigin` will configure `wal-listener` to only publish transactions which originate on `Database B`, and ignore those that originate on `Database A`.

#### `listener.maxTransactionSize`

By default, `wal-listener` will process all actions in a transaction. However, for very large transactions, you may want to limit the number of actions processed to avoid memory issues or overwhelming downstream systems.

If you set `listener.maxTransactionSize` to a positive integer, `wal-listener` will only process that many actions from each transaction. Any remaining actions will be dropped, and a log message will be emitted indicating how many actions were dropped.

```yaml
listener:
  maxTransactionSize: 1000  # Only process up to 1000 actions per transaction
```

__Note__: `listener.maxTransactionSize` does not cause an increase in memory usage, regardless of the `listener.skipTransactionBuffering` option. If you need to run memory constrained, you can use any combination of these options.

#### `listener.topicsMap`

TODO

## Monitoring

### Sentry

If you specify an DSN-string for the [Sentry](https://sentry.io/) project, the next level errors will be posted there via a hook:
* Panic
* Fatal
* Error

### Prometheus

You can take metrics by specifying an endpoint for Prometheus in the configuration.

#### Available metrics

| name                        | description                          | fields             |
|-----------------------------|--------------------------------------|--------------------|
| published_events_total      | the total number of published events | `subject`, `table` |
| filter_skipped_events_total | the total number of skipped events   | `table`            |

### Kubernetes

The `wal-listener` runs a web server (*if a port is specified in the configuration*) with two endpoints for readiness `/ready`  and liveness `/healthz` probes.

## Docker

You can start the container from the project folder (configuration file is required).

See `./config_example.yml` for an example configuration.
Be sure to copy the file to the docker image in the `Dockerfile` prior to running [after the build setp](https://github.com/ihippik/wal-listener/blob/master/Dockerfile#L31)
ex:
```docker
COPY /config.yml .
```

Сontainer preparation is carried out with the help of a multi-stage build, which creates after itself auxiliary images of a large size.
Please don't forget to delete them:

```shell
docker image prune --filter label=stage=builder
```