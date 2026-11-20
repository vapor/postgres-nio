import Metrics
import NIOCore

/// Records the query metrics of a connection.
///
/// Metrics are created from `MetricsSystem.factory` in the task that runs the query, so a factory bound with
/// `withMetricsFactory` applies to that query. That's also why timers can't be kept on the connection.
struct PostgresMetrics: Sendable {
    /// `db.system.name`, `db.namespace`, `server.address` and `server.port`, which every query reports.
    private let baseDimensions: [(String, String)]

    init(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?) {
        var dimensions = [("db.system.name", "postgresql")]
        if let database = configuration.database {
            dimensions.append(("db.namespace", database))
        }
        func appendServer(address: String, port: Int?) {
            dimensions.append(("server.address", address))
            // The conventions only ask for the port when it isn't the default one.
            if let port, port != 5432 {
                dimensions.append(("server.port", String(port)))
            }
        }
        switch configuration.connection {
        case .unresolvedTCP(let host, let port):
            appendServer(address: host, port: port)
        case .unresolvedUDS(let path):
            appendServer(address: path, port: nil)
        case .resolved(let socketAddress):
            if let address = socketAddress.ipAddress ?? socketAddress.pathname {
                appendServer(address: address, port: socketAddress.port)
            }
        case .bootstrapped:
            if let remoteAddress, let address = remoteAddress.ipAddress ?? remoteAddress.pathname {
                appendServer(address: address, port: remoteAddress.port)
            }
        }
        self.baseDimensions = dimensions
    }

    /// The instant to measure latency from, or `nil` when no metrics backend is set up, so that no work is done
    /// for metrics nobody collects.
    func startTiming() -> NIODeadline? {
        MetricsSystem.factory is NOOPMetricsHandler ? nil : .now()
    }

    /// Record a latency sample for a query if metrics timing was started.
    ///
    /// - Parameters:
    ///   - start: The instant returned by ``startTiming()``, or `nil` when timing never started.
    ///   - error: The error the query failed with, or `nil` when it succeeded.
    func record(start: NIODeadline?, error: (any Error)?) {
        guard let start else { return }
        let elapsed = NIODeadline.now() - start
        var dimensions = self.baseDimensions
        if let error {
            let psqlError = error as? PSQLError
            dimensions.append(("error.type", psqlError?.metricsErrorType ?? "_OTHER"))
            if let statusCode = psqlError?.metricsStatusCode {
                dimensions.append(("db.response.status_code", statusCode))
            }
        }
        Timer(label: "db.client.operation.duration", dimensions: dimensions).recordNanoseconds(elapsed.nanoseconds)
    }
}

extension PSQLError {
    /// The value to report as `error.type`: `cancelled` for cancelled queries, otherwise the
    /// ``metricsStatusCode``. `nil` when the error carries no recognisable code.
    var metricsErrorType: String? {
        self.code == .queryCancelled ? "cancelled" : self.metricsStatusCode
    }

    /// The SQLSTATE to report as `db.response.status_code`, or `nil` when the error didn't come from the server.
    ///
    /// A SQLSTATE is accepted only when it looks like one - five upper-case ASCII letters or digits, such as
    /// `42P01` - so that a server returning unusual text cannot inflate the cardinality of the metric.
    var metricsStatusCode: String? {
        guard self.code == .server, let sqlState = self.serverInfo?[.sqlState] else {
            return nil
        }
        guard sqlState.count == 5, sqlState.allSatisfy({ $0.isASCII && ($0.isUppercase || $0.isNumber) }) else {
            return nil
        }
        return sqlState
    }
}
