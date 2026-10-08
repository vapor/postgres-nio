import Metrics

/// A configuration object that defines the metrics behavior of a Postgres client or connection.
///
/// Metrics follow the OpenTelemetry
/// [database metrics semantic conventions](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/).
public struct PostgresMetricsConfiguration: Sendable {
    /// The metrics factory to use, or `nil` to disable metrics.
    public let factory: (any MetricsFactory)?

    /// Creates a metrics configuration.
    ///
    /// - Parameter factory: The factory used to create metrics. Defaults to `MetricsSystem.factory` at the time
    ///   the configuration is created: a task-local factory bound with `withMetricsFactory`, or the globally
    ///   bootstrapped one. Pass `nil` to disable metrics. A `NOOPMetricsHandler` also disables metrics, so that no
    ///   work is done for metrics nobody collects.
    public init(factory: (any MetricsFactory)? = MetricsSystem.factory) {
        self.factory = factory is NOOPMetricsHandler ? nil : factory
    }
}
