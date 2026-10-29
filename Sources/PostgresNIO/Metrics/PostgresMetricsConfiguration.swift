import Metrics

/// A configuration object that defines the metrics behavior of a Postgres client or connection.
///
/// Metrics follow the OpenTelemetry
/// [database metrics semantic conventions](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/).
public struct PostgresMetricsConfiguration: Sendable {
    /// The metrics factory to use, or `nil` to disable metrics.
    public let factory: (any MetricsFactory)?

    /// The labels of the metrics recorded by PostgresNIO. Defaults to OpenTelemetry semantics.
    public var labels: Labels = .init()

    /// The dimensions used on metrics recorded by PostgresNIO. Defaults to OpenTelemetry semantics.
    public var dimensions: Dimensions = .init()

    /// Labels of the metrics recorded by PostgresNIO.
    public struct Labels: Sendable {
        public var operationDuration: String = "db.client.operation.duration"
        public var connectionCount: String = "db.client.connection.count"
        public var connectionMax: String = "db.client.connection.max"
        public var connectionIdleMax: String = "db.client.connection.idle.max"
        public var connectionIdleMin: String = "db.client.connection.idle.min"
        public var pendingRequests: String = "db.client.connection.pending_requests"
        public var createTime: String = "db.client.connection.create_time"
        public var waitTime: String = "db.client.connection.wait_time"
        public var useTime: String = "db.client.connection.use_time"

        /// Creates the default metric labels.
        public init() {}
    }

    /// Dimensions used on metrics recorded by PostgresNIO.
    ///
    /// Each `Key` is the dimension name. A `Value` is provided only for dimensions with a static value;
    /// the others are filled in per connection or per operation.
    public struct Dimensions: Sendable {
        /// The dimension identifying the database system.
        public var databaseSystemKey: String = "db.system.name"
        /// The value reported for ``databaseSystemKey``.
        public var databaseSystemValue: String = "postgresql"

        /// The dimension carrying the database the connection was configured with.
        public var databaseNamespaceKey: String = "db.namespace"

        /// The dimension carrying the server's host name, IP address or socket path.
        public var serverAddressKey: String = "server.address"
        /// The dimension carrying the server's port. Omitted for the default port, `5432`.
        public var serverPortKey: String = "server.port"

        /// The dimension carrying the error a failed operation reported. Omitted on success.
        public var errorTypeKey: String = "error.type"
        /// Reported for ``errorTypeKey`` when an error carries no recognisable code.
        public var otherErrorTypeValue: String = "_OTHER"
        /// The dimension carrying the SQLSTATE of a failed operation. Omitted when there is none.
        public var statusCodeKey: String = "db.response.status_code"

        /// The dimension identifying the connection pool.
        public var connectionPoolNameKey: String = "db.client.connection.pool.name"
        /// The value reported for ``connectionPoolNameKey``. Must be unique within an application, so set it
        /// when an application runs more than one ``PostgresClient``.
        public var connectionPoolNameValue: String = "postgresnio-connection-pool"

        /// The dimension carrying a pool connection's state: `idle` or `used`.
        public var connectionStateKey: String = "db.client.connection.state"

        /// Creates the default dimensions.
        public init() {}
    }

    /// Creates a metrics configuration.
    ///
    /// - Parameter factory: The factory used to create metrics. Defaults to the globally bootstrapped
    ///   `MetricsSystem.factory`, read when the configuration is created, so bootstrap the metrics system first.
    ///   Pass `nil` to disable metrics. A `NOOPMetricsHandler` also disables metrics, so that no work is done for
    ///   metrics nobody collects.
    public init(factory: (any MetricsFactory)? = MetricsSystem.factory) {
        self.factory = factory is NOOPMetricsHandler ? nil : factory
    }
}
