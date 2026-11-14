# Tracing in PostgresNIO

## Summary

We want to collect traces about the client. This essentially means instrumenting PostgresNIO with swift-distributed-tracing and emitting spans for the queries we run. Together with metrics, this will allow us to understand which queries are slow and what the application was doing while executing it, instead of just showing that queries are slow.

## Motivation

Currently, a user can see that a request was slow, but not which query made it slow, or whether the time went on waiting for a connection.

## Operations

The spans listed here are taken from the [OTel Semantic Conventions for database clients](https://opentelemetry.io/docs/specs/semconv/db/database-spans/).
We emit one span per:
- structured `query` call (`SELECT`, `INSERT`, `DELETE`, and so on);
- prepared statement preparation;
- `COPY ... FROM STDIN` import;
- `withTransaction` block.

Span kind is `CLIENT` for everything that is a round trip to the server. The `withTransaction` scope is `INTERNAL` because it is only a scope, not an actual round trip. Its `BEGIN`, `COMMIT`, `ROLLBACK` operations and every query inside the block become its children spans.

> [!NOTE]
> While `LISTEN` and `UNLISTEN` are normal statements and require no extra work other than the ordinary query spans, incoming notifications are more complicated as there's no caller and no active task when a notification arrives, so we cannot attach a span to anything.

### Only structured query APIs are covered

Since a span must end when the rows have been consumed rather than when the query method returns, and we now have structured query APIs that look like 
```swift
func query<Result>(
    _ query: PostgresQuery,
    summary: String? = nil,
    _ body: (PostgresRowSequence) async throws -> Result
) async throws -> Result
```
For queries, tracing will only be implemented on those methods, leaving the old `EventLoopFuture` based methods and the `PostgresRowSequence`-returning APIs without tracing.

### Status

Span status follows [Recording errors](https://opentelemetry.io/docs/specs/semconv/general/recording-errors/). It is left unset on success and set to error on failure.

### Naming
The spec defines a fallback chain for the span name:
1. `{db.query.summary}` if a summary is available (e.g. `SELECT orders`);
2. `{db.operation.name} {target}`  if a low-cardinality operation name is available;
3. `{target}`, the first available of :
	1. `db.collection.name`
	2. `db.stored_procedure.name`
	3. `db.namespace`
	4. `server.address:server.port`
4. `{db.system.name}` (`postgresql` by default)

The PSQL conventions say `db.operation.name` should only be recorded "if the operation is executed via a higher-level API that does not support multiple operation names", and that it "SHOULD NOT be extracted from `db.query.text`". We therefore never parse SQL to obtain it. What we can know without touching the query text is:
- prepared statements: we can read it from the Swift type name (`PostgresPreparedStatement.name`);
- `COPY`: the target table comes from the API, so `db.collection.name` is set and `db.operation.name` is `COPY`;
- transactions: we write `BEGIN`, `COMMIT` and `ROLLBACK` ourselves, so naming them is not extraction. The scope span is named `TRANSACTION`.

For ordinary queries we add an optional `summary` parameter to the structured API, so a caller that knows the shape of its query can supply a low cardinality name. This would be handy when using an ORM such as Fluent, we could simply get the name from it and plug it into PostgresNIO. The new summary parameter must not contain values, this will be made clear in the parameter's doc comment.
When no summary is given we fall down the naming chain and the span is named after the target, which in practice is the database name.

The same parameter supplies `db.query.summary` to the metrics implementation, which lists the attribute but cannot currently populate it.

### Errors

`error.type` and `db.response.status_code` are set on failure only and are produced by the same code the metrics implementation uses, so that a user who finds a spike in `db.client.operation.duration` for one `error.type` can pivot to the spans behind it:

| Failure                                                       | `error.type`                      | `db.response.status_code` |
| ------------------------------------------------------------- | --------------------------------- | ------------------------- |
| `PSQLError` with `code == .server` and a well formed SQLSTATE | the SQLSTATE, for example `23505` | the SQLSTATE              |
| `PSQLError` with `code == .queryCancelled`                    | `cancelled`                       | not set                   |
| Any other error                                               | `_OTHER`                          | not set                   |

We only accept a SQLSTATE that is five uppercase letters or digits.

We do not attach the error itself to the span, and we do not copy the server's error fields onto it. Postgres appends user data into the primary message, not only into `DETAIL` and `HINT`, and we want to avoid leaking user data in the traces. Postgres does not provide a deterministic list of attributes which contain user data, so we try to take the safe path. This also means we must not use `Tracer.withSpan`, which records the thrown error on the span automatically. See Design.

### Attributes

The table below applies to query, prepared statement and `COPY` spans. `db.system.name`, `db.namespace`, `db.query.text`, `db.query.summary`, `db.operation.name`, `server.address` and `server.port` are set at span creation time.

| Key                         | Requirement level                                                                                                | Type   | Description                                                                                         | Example                              |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------ | --------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `db.system.name`            | Required                                                                                                         | string | Configurable, defaults to `postgresql`.                                                             | `postgresql`                         |
| `db.namespace`              | Conditionally required, if available without an additional network call                                          | string | The schema associated with the connection, qualified by the database name.                          | `mydatabase` |
| `db.response.status_code`   | Conditionally required, if the response ended with a warning or an error                                         | string | [PostgreSQL error code](https://www.postgresql.org/docs/current/errcodes-appendix.html) (SQLSTATE). | `08P01`; `23505`                     |
| `error.type`                | Conditionally required, if and only if the operation failed                                                      | string | Class of error. See Errors.                                                                         | `23505`; `cancelled`; `_OTHER`       |
| `server.port`               | Conditionally required, if not 5432 and `server.address` is set                                                  | int    | Server port number.                                                                                 | `6432`                               |
| `db.collection.name`        | Recommended, if the operation is executed via a higher-level API that does not support multiple collection names | string | The name of a table. Never extracted from `db.query.text`.                                          | `customers`                          |
| `db.operation.name`         | Recommended, if derivable from the API surface                                                                   | string | The operation being executed. See Naming.                                                           | `COPY`; `PREPARE`                    |
| `db.query.summary`          | Recommended, if supplied by the caller                                                                           | string | Low cardinality summary of the query.                                                               | `SELECT users`; `get user by id`     |
| `db.query.text`             | Recommended                                                                                                      | string | The query as sent, verbatim, with native placeholders.                                              | `SELECT * FROM users WHERE id = $1`  |
| `server.address`            | Recommended                                                                                                      | string | Name of the database host, or the socket path.                                                      | `example.com`; `/tmp/.s.PGSQL.5432`  |
| `network.peer.address`      | Recommended                                                                                                      | string | Peer address of the node the operation ran against.                                                 | `10.1.2.80`                          |
| `network.peer.port`         | Recommended, if and only if `network.peer.address` is set                                                        | int    | Peer port of the network connection.                                                                | `65123`                              |
| `db.response.returned_rows` | Opt-In                                                                                                           | int    | Number of rows returned by the operation.                                                           | `30`                                 |

Notes: 
  - Prepared statement preparation: `db.query.text` is the statement being prepared and `db.operation.name` is `PREPARE`. No `db.response.returned_rows`.
  - `COPY`: `db.collection.name` is known from the API and is set. No `db.query.summary`.
  - Transaction scope: `db.system.name`, `db.namespace` and `server.*` only. No query-level attributes as those live on the child spans.

Left out:
  - We do not produce [`db.query.parameter.<key>`](https://opentelemetry.io/docs/specs/semconv/registry/attributes/db/#db-query-parameter). This attribute includes the bind parameters used in the query and therefore possibly sensitive data. While it can be useful for debugging, users should rely on other methods to check their input, as leaving it enabled in production by mistake could leak that data.
  - We also do not produce `db.stored_procedure.name`. PostgresNIO has no stored procedure API, so the only way to learn the name would be to parse `db.query.text`, which the conventions forbid. The attribute is unreachable for us.

#### `db.namespace`

The spec defines it as `{database}|{schema}`, and if one part is missing the other is used on its own.
We only report the database name. Reading the live schema means running `SELECT current_schema()`, and the requirement level excludes attributes that need an extra network call. A Postgres connection cannot switch database, so the value stays correct for the whole life of the connection.

#### `db.query.text`

We record the query text as we already hold it, including the native `$1`, `$2` placeholders. [The spec](https://opentelemetry.io/docs/specs/semconv/db/database-spans/#sanitization-of-dbquerytext) says parametrised query text should be collected by default and should not be sanitised.
This also means that using `PostgresQuery(unsafeSQL:)` might leak sensitive information if literals are interpolated in the string. We should explicitly mention this in the initialiser's doc comment.

## Design

Spans are emitted by the connection, not by the client. Queries can be run on a standalone `PostgresConnection`, and the connection is also where the peer address is known. `PostgresConnection` therefore will hold an optional `PostgresTracing`, built once during `connect` and reused for every span:
```swift
let tracing: PostgresTracing

init(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?)
```
`PostgresTracing` holds the attributes that depend on the connection and the span name target. None of these need to be rebuilt per-query. Where possible, logic will be extracted from `PostgresMetrics` to compute values, such as `server.port`, and shared with `PostgresTracing`. Metrics turns the result into dimensions while tracing turns it into `SpanAttributes`.

### Span lifetime

The span is created with `startSpan` and ended in a `defer`. We do not use `Tracer.withSpan` since it records the thrown error on the span, which is exactly what the Error section forbids.
The span starts once the connection is available, so time spent waiting for a pooled connection is not part of it. The `db.client.connection.wait_time` metric covers that wait.

A query span lasts until `body` returns, but it is not the current `ServiceContext` while `body` runs. Spans the caller creates inside `body` are therefore siblings of the query span, not its children, as with valkey-swift. The transaction span instead is the current context for the whole `withTransaction` block, so `process` runs inside it and the queries and spans it creates are children of the transaction span.

### Returned rows

`db.response.returned_rows` is counted on `PSQLRowStream` as rows arrive from the server and read when `body` returns. The counter will be an atomic because the event loop writes it and the caller's task reads it.
The count is not always a total: in the case that `body` returns early while the server is still sending, the count will only be the rows returned up to that point.
The attribute is opt-in since it's still in development in the spec.

### Configuration

Tracing is configurable and enabled by default through a new `tracing` property on the options of both `PostgresClient.Configuration` and `PostgresConnection.Configuration`:
```swift
var configuration = PostgresClient.Configuration(...)
configuration.options.tracing.recordsQueryText = false
configuration.options.tracing.databaseSystemName = "cockroachdb"
```

```swift
public struct PostgresTracingConfiguration: Sendable {
    /// Whether the query's SQL is recorded as `db.query.text`. Defaults to `true`.
    public var recordsQueryText: Bool = true

    /// Whether the number of rows received is recorded as `db.response.returned_rows`. Defaults to `false`.
    public var recordsReturnedRows: Bool = false

    /// The value of `db.system.name`. Defaults to `postgresql`.
    public var databaseSystemName: String = "postgresql"

    public init() {}
}
```
Every operation reads `InstrumentationSystem.tracer`, which returns the tracer bound with `withTracer(_:_:)` or else the bootstrapped one.
The read happens per operation and not once per connection, so that a tracer set with `withTracer` is respected for each individual operation.
A client therefore cannot have its own tracer, and there is no per-client switch to turn tracing off. An operation that should not be traced runs inside `withTracer(NoOpTracer())`. The operation checks for a `NoOpTracer` before starting a span, so no span is allocated when nobody is collecting spans.

`db.system.name` defaults to `postgresql`. It can be changed for a different database that speaks the Postgres protocol, for example `cockroachdb`, which has its own value in the conventions. The attribute names themselves are not configurable.

## Alternatives considered

### Inferring the operation name from the query
We could take the first keyword of the query text, match it against a known set such as `SELECT` and `UPDATE`, and use it as `db.operation.name`. This would give better span names for hand written queries with no summary. The PSQL conventions explicitly forbid deriving the attribute from `db.query.text`, and a parser that is wrong on a comment, a CTE or a leading whitespace variant produces a misleading low cardinality attribute, so we do not do this.

### Not tracing

The obvious other choice was not emitting spans at all. However, as the ecosystem matures and PostgresNIO is the default library for interacting with a Postgres database, instrumentation is a very valuable resource we should support.
