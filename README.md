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

### Logic of work
To receive events about data changes in our PostgreSQL DB
  we use the standard logic decoding module (**pgoutput**) This module converts
 changes read from the WAL into a logical replication protocol.
  And we already consume all this information on our side.
Then we filter out only the events we need and publish them in the queue

### Event publishing

NATS Streaming is used as a message broker.
Service publishes the following structure.
The name of the topic for subscription to receive messages is formed from the prefix of the topic, 
the name of the database and the name of the table `prefix + schema_table`.

```
{
	ID        uuid.UUID   # unique ID           
	Schema    string                 
	Table     string                 
	Action    string                 
	Data      map[string]interface{} 
	EventTime time.Time   # commit time          
}
```

Messages are published to Nats-Streaming at least once!

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

### DB setting
You must make the following settings in the db configuration (postgresql.conf)
* wal_level >= “logical”
* max_replication_slots >= 1

The publication & slot created automatically when the service starts (for all tables and all actions). 
You can delete the default publication and create your own (name: _wal-listener_) with the necessary filtering conditions, and then the filtering will occur at the database level and not at the application level.

https://www.postgresql.org/docs/current/sql-createpublication.html

If you change the publication, do not forget to change the slot name or delete the current one.

### Docker

You can start the container from the project folder (configuration file is required)

```
docker run -v $(pwd)/config.yml:/app/config.yml ihippik/wal-listener:pgoutput
```