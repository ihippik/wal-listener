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
and publish them to the NATS Streaming server.

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

Service publishes the following structure.
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
By default, output NATS topic name consist of prefix, DB schema, and DB table name,
but if you want to send all update in one topic you should be configured the topic map:
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

## Service configuration
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

### Kubernetes
Application initializes a web server (*if a port is specified in the configuration*) with two endpoints 
for readiness `/ready`  and liveness `/healthz` probes.

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
