import Metrics
import NIOConcurrencyHelpers
import NIOCore

struct PostgresMetrics: Sendable {
    private let factory: any MetricsFactory
    private let operationDurationLabel: String
    private let errorTypeKey: String
    private let statusCodeKey: String
    private let otherErrorTypeValue: String
    /// `db.system.name`, `db.namespace`, `server.address` and `server.port`, which every query reports.
    private let baseDimensions: [(String, String)]
    /// Successful queries carry no `error.type`, so they always use the same timer.
    private let successTimer: Timer
    /// Keyed by `error.type`. `db.response.status_code` needs no key of its own, as it is derived from the same
    /// error as `error.type`.
    private let errorTimers = NIOLockedValueBox<[String: Timer]>([:])

    /// Creates the handles a connection records into, or `nil` when metrics emission is disabled.
    init?(configuration: PostgresConnection.InternalConfiguration, remoteAddress: SocketAddress?) {
        let metrics = configuration.options.metrics
        guard let factory = metrics.factory else { return nil }
        self.factory = factory
        self.operationDurationLabel = metrics.labels.operationDuration
        self.errorTypeKey = metrics.dimensions.errorTypeKey
        self.statusCodeKey = metrics.dimensions.statusCodeKey
        self.otherErrorTypeValue = metrics.dimensions.otherErrorTypeValue

        var dimensions = [(metrics.dimensions.databaseSystemKey, metrics.dimensions.databaseSystemValue)]
        if let database = configuration.database {
            dimensions.append((metrics.dimensions.databaseNamespaceKey, database))
        }
        func appendServer(address: String, port: Int?) {
            dimensions.append((metrics.dimensions.serverAddressKey, address))
            // The conventions only ask for the port when it isn't the default one.
            if let port, port != 5432 {
                dimensions.append((metrics.dimensions.serverPortKey, String(port)))
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
        self.successTimer = Timer(label: metrics.labels.operationDuration, dimensions: dimensions, factory: factory)
    }

    /// The instant to measure latency from.
    func startTiming() -> NIODeadline {
        .now()
    }

    /// Record a latency sample for a query if metrics timing was started.
    ///
    /// - Parameters:
    ///   - start: The instant returned by ``startTiming()``, or `nil` when timing never started.
    ///   - error: The error the query failed with, or `nil` when it succeeded.
    func record(start: NIODeadline?, error: (any Error)?) {
        guard let start else { return }
        let elapsed = NIODeadline.now() - start
        if let error {
            let psqlError = error as? PSQLError
            self.errorTimer(
                errorType: psqlError?.metricsErrorType ?? self.otherErrorTypeValue,
                statusCode: psqlError?.metricsStatusCode
            ).recordNanoseconds(elapsed.nanoseconds)
        } else {
            self.successTimer.recordNanoseconds(elapsed.nanoseconds)
        }
    }

    /// The timer for a failure, created from the connection's factory on first use.
    private func errorTimer(errorType: String, statusCode: String?) -> Timer {
        self.errorTimers.withLockedValue { timers in
            if let cached = timers[errorType] {
                return cached
            }
            var dimensions = self.baseDimensions
            dimensions.append((self.errorTypeKey, errorType))
            if let statusCode {
                dimensions.append((self.statusCodeKey, statusCode))
            }
            let timer = Timer(label: self.operationDurationLabel, dimensions: dimensions, factory: self.factory)
            timers[errorType] = timer
            return timer
        }
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
