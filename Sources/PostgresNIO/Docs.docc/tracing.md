# Tracing

Emit distributed tracing spans for Postgres operations with ``PostgresConnection`` and ``PostgresClient``.

## Overview

PostgresNIO supports opt-in distributed tracing using [swift-distributed-tracing](https://github.com/apple/swift-distributed-tracing). When enabled, PostgresNIO creates database client spans that follow OpenTelemetry database conventions as closely as possible without adding another semantic-conventions dependency.

Tracing is disabled by default. Once enabled, PostgresNIO uses the bootstrapped global tracer from `InstrumentationSystem.tracer` unless you provide an explicit tracer override in ``PostgresTracingConfiguration``.

## Enable Tracing

Bootstrap your tracer provider first, then enable tracing in the connection or client options:

```swift
import PostgresNIO
import Tracing

InstrumentationSystem.bootstrap(MyTracer())

var clientConfiguration = PostgresClient.Configuration(
    host: "localhost",
    port: 5432,
    username: "username",
    password: "password",
    database: "my_database",
    tls: .disable
)
clientConfiguration.options.tracing.isEnabled = true

var connectionConfiguration = PostgresConnection.Configuration(
    host: "localhost",
    port: 5432,
    username: "username",
    password: "password",
    database: "my_database",
    tls: .disable
)
connectionConfiguration.options.tracing.isEnabled = true
```

To override the tracer used by a specific client or connection, assign ``PostgresTracingConfiguration/tracer`` directly:

```swift
clientConfiguration.options.tracing.tracer = myTracer
```

## Query Text Policy

``PostgresTracingConfiguration`` defaults `db.query.text` to a safe policy.

- Parameterized SQL is recorded by default when PostgresNIO has actual bind values for the query.
- Library-generated SQL, such as the generated `COPY` statement used by `copyFrom`, may be recorded with a sanitized tracing form.
- Raw non-parameterized SQL strings are not attached unless you opt in.

If you want PostgresNIO to attach raw SQL text to all traced operations, set:

```swift
clientConfiguration.options.tracing.queryTextPolicy = .recordAll
```

Use `.recordAll` only when raw query text is acceptable for your deployment and data handling requirements.

## Statement Metadata Policy

``PostgresTracingConfiguration`` defaults statement metadata to an exact mode.

- Exact metadata is emitted only when PostgresNIO already knows the operation from the higher-level API surface, such as explicit `prepare`, explicit `deallocate`, and `copyFrom`.
- Generic `query` and prepared execution spans do not derive `db.operation.name` from SQL text by default.
- Generic span names therefore usually fall back to the database namespace when available.

If you want compatibility with backends that benefit from SQL verb grouping on generic queries, opt in to heuristic SQL keyword inference:

```swift
clientConfiguration.options.tracing.statementMetadataPolicy = .inferred
```

This infers `db.operation.name` from the first keyword of the SQL text (for example `SELECT`, `INSERT`, `UPDATE`) and uses that to build the span name. It is intended for observability compatibility, not exact semantic understanding.

If you do not want optional statement metadata at all, disable it:

```swift
clientConfiguration.options.tracing.statementMetadataPolicy = .disabled
```

When disabled, PostgresNIO omits `db.operation.name` and `db.query.summary`. Generic span names still fall back to the namespace or server target when available.

## Error Details Policy

``PostgresTracingConfiguration`` defaults recorded tracing exceptions to a safe error description.

- `PSQLError` keeps its generic privacy-preserving description by default.
- `error.type` and `db.response.status_code` are still attached to the span when available.
- More detailed exception messages are opt-in.

If you want PostgresNIO to attach the primary PostgreSQL server error message to failed spans, set:

```swift
clientConfiguration.options.tracing.errorDetailsPolicy = .message
```

If you want PostgresNIO to attach `String(reflecting: error)` to failed spans, set:

```swift
clientConfiguration.options.tracing.errorDetailsPolicy = .debugDescription
```

`.debugDescription` may include sensitive information such as server detail strings, query context, and source locations, so it should only be enabled when that additional visibility is acceptable for your deployment.

## Emitted Spans

This release traces:

- `query`
- prepared statement execution
- explicit `prepare`
- explicit `deallocate`
- `copyFrom`
- `withTransaction`

`PostgresClient` starts spans before leasing a pooled connection, so queue wait is included in the measured duration. `PostgresConnection` starts spans when the operation is invoked.

Async query spans stay open until the returned ``PostgresRowSequence`` is fully consumed or terminated early. Callback-based row handlers restore the span's `ServiceContext` while your callback runs so that logs and child spans created there attach to the database span.

Managed transactions produce an additional INTERNAL wrapper span named `postgres.transaction`. The `BEGIN`, inner query spans, and `COMMIT` or `ROLLBACK` spans are children of that wrapper span.

## Span Names And Attributes

Span names follow this order:

- `db.query.summary` when exact summary metadata is available
- `{db.operation.name} {target}` when operation metadata is available and a target is known
- `{target}` for generic query spans without operation metadata
- `postgresql` only when no better target is available

The target prefers `db.collection.name`, then `db.stored_procedure.name`, then `db.namespace`, and finally `server.address:server.port`.

Common attributes include:

- `db.system.name = "postgresql"`
- `db.namespace`
- `db.query.text`
- `db.operation.name` — exact by default, or heuristically inferred from SQL when ``PostgresTracingConfiguration/StatementMetadataPolicy/inferred`` is enabled
- `db.query.summary` — only for exact operations whose low-cardinality summary is already known
- `server.address`
- `server.port`
- `network.peer.address`
- `network.peer.port`

On failures, PostgresNIO marks the span as an error and records:

- `db.response.status_code` from SQLSTATE when available
- `error.type` from SQLSTATE or the canonical client-side error type
- an exception message according to ``PostgresTracingConfiguration/ErrorDetailsPolicy``

PostgresNIO does not record query parameter values in this version.

## Exclusions

The first tracing version intentionally does not create spans for:

- connection establishment
- authentication
- pool maintenance
- `LISTEN` / `NOTIFY`

Those areas can be added later without changing the opt-in tracing configuration surface.
