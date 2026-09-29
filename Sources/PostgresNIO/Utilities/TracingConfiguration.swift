import Tracing

/// Configures the spans the client emits for queries, following the OpenTelemetry
/// [database span semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/).
public struct TracingConfiguration: Sendable {
    public static let `default` = Self()

    /// Whether the client creates spans at all. Defaults to `true`.
    public var isEnabled: Bool = true

    /// The tracer used to create spans. Defaults to the globally bootstrapped `InstrumentationSystem.tracer`.
    public var tracer: any Tracer = InstrumentationSystem.tracer

    /// Whether the query's SQL is recorded as `db.query.text`. Defaults to `true`.
    ///
    /// The recorded text is the SQL as sent to the server, with `$n` placeholders in place of the
    /// bound values, so it does not contain bind values. Disable this if you interpolate sensitive
    /// data directly into your SQL.
    public var recordsQueryText: Bool = true

    /// Whether the number of rows received from the server is recorded as `db.response.returned_rows`.
    /// Defaults to `false`, as the attribute is opt-in and still in development in the spec.
    public var recordsReturnedRows: Bool = false

    public init() {}
}
