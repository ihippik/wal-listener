# Agency guide for WAL-Listener

This document gives coding agents and maintainers a compact operating guide for working in this repository.

## Project overview

WAL-Listener is a Go service for event-driven architectures. It consumes PostgreSQL logical replication changes through `pgoutput`, filters relevant table actions, and publishes committed change events to a configured message broker.

Supported publishers:

- NATS JetStream (`type=nats`)
- Apache Kafka (`type=kafka`)
- RabbitMQ (`type=rabbitmq`)
- Google Pub/Sub (`type=google_pubsub`)

Delivery is at-least-once, so changes must preserve idempotency expectations for downstream consumers.

## Core behavior to preserve

When changing runtime logic, keep these invariants in mind:

- PostgreSQL WAL/logical-decoding handling must only publish committed database changes.
- Event shape must remain compatible unless the change is explicitly documented as breaking.
- Filtering semantics must stay aligned with configured table/action filters: `insert`, `update`, `delete`, and `truncate`.
- Topic naming and `topicsMap` behavior must stay backward compatible.
- Broker implementations should expose equivalent behavior where practical, while respecting broker-specific semantics such as Kafka message keys and RabbitMQ exchange kinds.
- Configuration keys are camelCase, except database entity names.
- Environment overrides use the `WAL_` prefix.
- TLS behavior for PostgreSQL must preserve `enableTLS` and `sslMode` semantics: `require`, `verify-ca`, and `verify-full`.

## Development workflow

Before making changes:

1. Read `README.md` and `config_example.yml` for user-facing behavior and configuration contracts.
2. Inspect the package you are changing before editing adjacent abstractions.
3. Prefer small, focused commits that are easy to review.

Before opening a pull request, run the relevant checks locally when possible:

```sh
go test ./...
go test -race ./...
gofmt -w <changed-go-files>
go mod tidy
```

Only commit `go.mod` or `go.sum` changes when dependency changes are intentional.

## Coding guidelines

- Keep exported APIs, config fields, metric names, and event fields stable unless the PR clearly explains a breaking change.
- Prefer context-aware operations for I/O, network clients, replication loops, and broker publishing.
- Do not swallow errors from PostgreSQL, broker clients, or configuration loading. Wrap errors with actionable context.
- Keep retry/reconnect logic explicit and observable through logs or metrics.
- Avoid introducing global mutable state unless it is part of process-wide initialization.
- Use structured logging consistently with the existing logger setup.
- Keep generated or mock files clearly separated from hand-written code.
- Do not add secrets, credentials, local config files, or environment-specific endpoints to the repository.

## Testing guidance

Add or update tests for changes in:

- replication message parsing
- table/action filtering
- event payload construction
- topic mapping
- publisher behavior
- configuration parsing and environment overrides
- TLS connection options
- metrics and health/readiness behavior

For broker-specific changes, cover both success and failure paths. For concurrency changes, prefer deterministic tests and run with `-race`.

## Documentation guidance

Update `README.md` and `config_example.yml` when changing:

- configuration keys or defaults
- event payload fields
- supported broker behavior
- topic naming or mapping rules
- PostgreSQL setup requirements
- Docker or Kubernetes usage
- monitoring metrics or probe endpoints

## Pull request checklist

Use this checklist in PR descriptions when applicable:

- [ ] User-facing behavior is documented.
- [ ] Configuration compatibility is preserved or migration notes are included.
- [ ] Tests cover the changed behavior.
- [ ] `go test ./...` passes.
- [ ] `gofmt` was run on changed Go files.
- [ ] No secrets or local-only config were committed.
