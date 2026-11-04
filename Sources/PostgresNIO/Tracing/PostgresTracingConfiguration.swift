import Tracing

/// Configures the spans PostgresNIO emits, following the OpenTelemetry
/// [database span semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/).
public struct PostgresTracingConfiguration: Sendable {
    /// The tracer used to create spans, or `nil` to disable tracing.
    /// A `NoOpTracer` disables tracing too, so no work is done when nobody collects spans.
    public var tracer: (any Tracer)?

    /// Whether the query's SQL is recorded as `db.query.text`. Defaults to `true`.
    ///
    /// The recorded text is the SQL as sent to the server, with `$n` placeholders in place of the
    /// bound values, so it does not contain bind values. Disable this if you interpolate sensitive
    /// data directly into your SQL.
    public var recordsQueryText: Bool = true

    /// Whether the number of rows received from the server is recorded as `db.response.returned_rows`.
    /// Defaults to `false`, as the attribute is opt-in and still in development in the spec.
    public var recordsReturnedRows: Bool = false

    /// The attribute names used in spans created by PostgresNIO. Defaults to the OpenTelemetry names.
    public var attributeNames: AttributeNames = .init()

    /// Creates a tracing configuration.
    ///
    /// - Parameter tracer: The tracer used to create spans. Defaults to `InstrumentationSystem.tracer`, which is
    ///   read when the configuration is created, so bootstrap the instrumentation system first.
    public init(tracer: (any Tracer)? = InstrumentationSystem.tracer) {
        self.tracer = tracer
    }

    /// Attribute names used in spans created by PostgresNIO.
    public struct AttributeNames: Sendable {
        public var databaseSystemName: String = "db.system.name"
        public var databaseNamespace: String = "db.namespace"
        public var databaseQueryText: String = "db.query.text"
        public var databaseQuerySummary: String = "db.query.summary"
        public var databaseOperationName: String = "db.operation.name"
        public var databaseCollectionName: String = "db.collection.name"
        public var databaseResponseStatusCode: String = "db.response.status_code"
        public var databaseResponseReturnedRows: String = "db.response.returned_rows"
        public var errorType: String = "error.type"
        public var serverAddress: String = "server.address"
        public var serverPort: String = "server.port"
        public var networkPeerAddress: String = "network.peer.address"
        public var networkPeerPort: String = "network.peer.port"
    }
}
