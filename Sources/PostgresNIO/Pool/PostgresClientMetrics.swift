import _ConnectionPoolModule
import Logging
import Metrics
import NIOConcurrencyHelpers

final class PostgresClientMetrics: ConnectionPoolObservabilityDelegate {
    typealias ConnectionID = PostgresConnection.ID

    struct PoolInstruments {
        let idleConnections: Gauge
        let usedConnections: Gauge
        let pendingRequests: Gauge
        let createTime: Timer
        let waitTime: Timer
        let useTime: Timer

        init(factory: any MetricsFactory, configuration: PostgresMetricsConfiguration, minimumConnections: Int, maximumConnections: Int) {
            let labels = configuration.labels
            let names = configuration.dimensions
            let pool = [(names.connectionPoolNameKey, names.connectionPoolNameValue)]
            
            // The limits never change, so they are recorded once
            Gauge(label: labels.connectionMax, dimensions: pool, factory: factory).record(maximumConnections)
            Gauge(label: labels.connectionIdleMax, dimensions: pool, factory: factory).record(maximumConnections)
            Gauge(label: labels.connectionIdleMin, dimensions: pool, factory: factory).record(minimumConnections)

            self.idleConnections = Gauge(label: labels.connectionCount, dimensions: pool + [(names.connectionStateKey, "idle")], factory: factory)
            self.usedConnections = Gauge(label: labels.connectionCount, dimensions: pool + [(names.connectionStateKey, "used")], factory: factory)
            self.pendingRequests = Gauge(label: labels.pendingRequests, dimensions: pool, factory: factory)
            self.createTime = Timer(label: labels.createTime, dimensions: pool, factory: factory)
            self.waitTime = Timer(label: labels.waitTime, dimensions: pool, factory: factory)
            self.useTime = Timer(label: labels.useTime, dimensions: pool, factory: factory)
        }
    }

    let logger: Logger
    let poolInstruments: PoolInstruments?
    private let lastStatisticsID = NIOLockedValueBox(0)

    init(logger: Logger, configuration: PostgresMetricsConfiguration, minimumConnections: Int, maximumConnections: Int) {
        self.logger = logger
        self.poolInstruments = configuration.factory.map {
            PoolInstruments(
                factory: $0,
                configuration: configuration,
                minimumConnections: minimumConnections,
                maximumConnections: maximumConnections
            )
        }
    }

    func report(statistics: ConnectionPoolStatistics) {
        guard let instruments = self.poolInstruments else { return }

        // Snapshots can arrive out of order
        self.lastStatisticsID.withLockedValue { lastID in
            guard statistics.id > lastID else { return }
            lastID = statistics.id

            instruments.idleConnections.record(statistics.idleConnectionCount)
            instruments.usedConnections.record(statistics.usedConnectionCount)
            instruments.pendingRequests.record(statistics.pendingRequests)
        }
    }

    func startedConnecting(id: ConnectionID) {
        self.logger.debug("Creating new connection", metadata: [
            .connectionID: "\(id)",
        ])
    }

    /// A connection attempt failed with the given error. After some period of
    /// time ``startedConnecting(id:)`` may be called again.
    func connectFailed(id: ConnectionID, error: any Error) {
        self.logger.info("Connection creation failed", metadata: [
            .connectionID: "\(id)",
            .error: "\(String(reflecting: error))"
        ])
    }

    func connectSucceeded(id: ConnectionID) {
        self.logger.debug("Connection established", metadata: [
            .connectionID: "\(id)"
        ])
    }

    /// The utlization of the connection changed; a stream may have been used, returned or the
    /// maximum number of concurrent streams available on the connection changed.
    func connectionLeased(id: ConnectionID) {
        self.logger.debug("Connection leased", metadata: [
            .connectionID: "\(id)"
        ])
    }

    func connectionReleased(id: ConnectionID) {
        self.logger.debug("Connection released", metadata: [
            .connectionID: "\(id)"
        ])
    }

    func keepAliveTriggered(id: ConnectionID) {
        self.logger.debug("run ping pong", metadata: [
            .connectionID: "\(id)",
        ])
    }

    func keepAliveSucceeded(id: ConnectionID) {}

    func keepAliveFailed(id: PostgresConnection.ID, error: any Error) {}

    /// The remote peer is quiescing the connection: no new streams will be created on it. The
    /// connection will eventually be closed and removed from the pool.
    func connectionClosing(id: ConnectionID) {
        self.logger.debug("Close connection", metadata: [
            .connectionID: "\(id)"
        ])
    }

    /// The connection was closed. The connection may be established again in the future (notified
    /// via ``startedConnecting(id:)``).
    func connectionClosed(id: ConnectionID, error: (any Error)?) {
        self.logger.debug("Connection closed", metadata: [
            .connectionID: "\(id)"
        ])
    }

    func requestQueueDepthChanged(_ newDepth: Int) {

    }

    func connectSucceeded(id: PostgresConnection.ID, streamCapacity: UInt16) {

    }

    func connectionUtilizationChanged(id: PostgresConnection.ID, streamsUsed: UInt16, streamCapacity: UInt16) {
        
    }
}
