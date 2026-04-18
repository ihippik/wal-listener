# WAL-Listener

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/ihippik/wal-listener)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/ihippik/wal-listener)
[![Publish Docker image](https://github.com/ihippik/wal-listener/actions/workflows/github-actions.yml/badge.svg)](https://github.com/ihippik/wal-listener/actions/workflows/github-actions.yml)

![WAL-Listener](wal-listener.png)

A service that helps implement the **Event-Driven architecture**.

To maintain the consistency of data in the system, we will use **transactional messaging** -
publishing events in a single transaction with a domain model change.

The service allows you to subscribe to changes in the PostgreSQL database using its logical decoding capability
and publish them to a message broker.

## Logic of work
To receive events about data changes in our PostgreSQL DB
  we use the standard logic decoding module (**pgoutput**) This module converts
changes read from the WAL into a logical replication protocol.
  And we already consume all this information on our side.
Then we filter out only the events we need and publish them in the queue

### Event publishing

As the message broker will be used is of your choice:
- NATS JetStream [`type=nats`];
- Apache Kafka [`type=kafka`];
- RabbitMQ [`type=rabbitmq`].
- Google Pub/Sub [`type=google_pubsub`].

The service publishes the following structure.
The name of the topic for subscription to receive messages is formed from the prefix of the topic,
the name of the database, and the name of the table `prefix + schema_table`.

> If you are using Kafka, you may want to select a partition using a message key.
> You can do this in the producer configuration by specifying the **messageKeyFrom** variable,
> which will indicate from which table field to take the key.
> If there is no such field, the table name will be used.

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

## Production checklist

Before running WAL-Listener in production, make sure that:

- PostgreSQL is configured for logical replication:
  - `wal_level >= logical`
  - `max_replication_slots >= 1`
- consumers are **idempotent**, because events are delivered **at least once**
- the replication slot name is treated as part of the deployment state
- Prometheus metrics and Kubernetes probes are enabled
- broker-specific connection and reconnect behavior is understood before rollout

## Delivery semantics

WAL-Listener publishes events with **at least once** delivery semantics.

This means that in normal operation each change should be published once, but duplicates are still possible during reconnects, retries, restarts, or broker-side failures. Consumers should therefore be designed to be idempotent.

Typical ways to achieve this:
- deduplicate by event ID
- deduplicate by business key and version
- make downstream writes naturally idempotent

WAL-Listener does **not** guarantee exactly-once delivery.

### Filter configuration example

```yaml
listener:
  filter:
    tables:
      users:
        - insert
        - update

```
This filter means that we only process events occurring with the `users` table,
and in particular `insert` and `update` data.

### Topic mapping
By default, the output NATS topic name consists of prefix, DB schema, and DB table name,
but if you want to send all updates in one topic, you should be configured the topic map:
```yaml
topicsMap:
  main_users: "notifier"
  main_customers: "notifier"
```

## DB setting
You must make the following settings in the db configuration (postgresql.conf)
* wal_level >= “logical”
* max_replication_slots >= 1

The publication & slot created automatically when the service starts (for all tables and all actions).
You can delete the default publication and create your own (name: _wal-listener_) with the necessary filtering conditions, and then the filtering will occur at the database level and not at the application level.

https://www.postgresql.org/docs/current/sql-createpublication.html

If you change the publication, do not forget to change the slot name or delete the current one.

Notes:

1. To receive `DataOld` field you need to change REPLICA IDENTITY to FULL as described here:
   [#SQL-ALTERTABLE-REPLICA-IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)

## Common pitfalls and troubleshooting

### `DataOld` is empty

To receive old row values in `DataOld`, the table must use `REPLICA IDENTITY FULL` as described in the PostgreSQL documentation:
[#SQL-ALTERTABLE-REPLICA-IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)

### Events behave unexpectedly after changing publication settings

If you change the publication, remember to use a new slot name or remove the old slot before restarting WAL-Listener. Replication slots preserve state and may continue from an unexpected position if reused after publication changes.

### Duplicate messages appear downstream

This is expected under **at least once** delivery semantics. Downstream consumers should be able to process the same event more than once without producing incorrect side effects.

### The process is running but no events are reaching the broker

Check:
- PostgreSQL logical replication settings
- publication and slot configuration
- broker connectivity
- application logs
- Prometheus metrics

If Kubernetes probes are enabled, also verify that readiness and liveness are configured correctly in the deployment.

## Service configuration
> All config entities are written in camelCase, except for the database entities themselves
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
We are using [Viper;](https://github.com/spf13/viper) it means you can override each value via env variables with `WAL_` prefix.

_for instance: `WAL_DATABASE_PORT=5433`_

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

## Kubernetes notes

If `listener.serverPort` is configured, WAL-Listener starts an HTTP server with:
- readiness probe: `/ready`
- liveness probe: `/healthz`

This is recommended for production deployments in Kubernetes so that orchestration can detect unhealthy instances and restart them when needed.

## Observability notes

For production usage, it is recommended to enable:

- Prometheus metrics via `monitoring.promAddr`
- Sentry via `monitoring.sentryDSN`
- structured logs via `logger.fmt: json`

Useful built-in metrics:
- `published_events_total`
- `filter_skipped_events_total`

A rising number of skipped events usually means the current table/action filter excludes incoming changes. A lack of published events together with active database changes may indicate connectivity or broker-side issues.

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

#### Docker Hub
https://hub.docker.com/r/ihippik/wal-listener
#### Example
```shell
docker run -v $(pwd)/config.yml:/app/config.yml ihippik/wal-listener:tag
```
