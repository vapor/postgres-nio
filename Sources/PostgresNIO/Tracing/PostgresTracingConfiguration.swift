/// Configures the spans PostgresNIO emits, following the OpenTelemetry
/// [database span semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/).
///
/// Each operation creates its span with `InstrumentationSystem.tracer`, which also picks up `withTracer(_:_:)`.
/// To turn tracing off for an operation, run it inside `withTracer(NoOpTracer())`.
public struct PostgresTracingConfiguration: Sendable {
    /// Whether the query's SQL is recorded as `db.query.text`. Defaults to `true`.
    ///
    /// The recorded text is the SQL as sent to the server, with `$n` placeholders in place of the
    /// bound values, so it does not contain bind values. Disable this if you interpolate sensitive
    /// data directly into your SQL.
    public var recordsQueryText: Bool = true

    /// Whether the number of rows received from the server is recorded as `db.response.returned_rows`.
    /// Defaults to `false`, as the attribute is opt-in and still in development in the spec.
    public var recordsReturnedRows: Bool = false

    /// The value of `db.system.name`. Defaults to `postgresql`.
    ///
    /// Change this when connecting to a different database that supports the Postgres protocol, for example
    /// `cockroachdb`.
    public var databaseSystemName: String = "postgresql"

    /// Creates a tracing configuration.
    public init() {}
}
