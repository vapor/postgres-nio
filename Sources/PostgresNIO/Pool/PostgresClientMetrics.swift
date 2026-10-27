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
        let maxConnections: Gauge
        let maxIdleConnections: Gauge
        let minIdleConnections: Gauge
        let createTime: Timer
        let waitTime: Timer
        let useTime: Timer

        init(factory: any MetricsFactory, poolName: String) {
            let pool = [("db.client.connection.pool.name", poolName)]
            self.idleConnections = Gauge(label: "db.client.connection.count", dimensions: pool + [("db.client.connection.state", "idle")], factory: factory)
            self.usedConnections = Gauge(label: "db.client.connection.count", dimensions: pool + [("db.client.connection.state", "used")], factory: factory)
            self.pendingRequests = Gauge(label: "db.client.connection.pending_requests", dimensions: pool, factory: factory)
            self.maxConnections = Gauge(label: "db.client.connection.max", dimensions: pool, factory: factory)
            self.maxIdleConnections = Gauge(label: "db.client.connection.idle.max", dimensions: pool, factory: factory)
            self.minIdleConnections = Gauge(label: "db.client.connection.idle.min", dimensions: pool, factory: factory)
            self.createTime = Timer(label: "db.client.connection.create_time", dimensions: pool, factory: factory)
            self.waitTime = Timer(label: "db.client.connection.wait_time", dimensions: pool, factory: factory)
            self.useTime = Timer(label: "db.client.connection.use_time", dimensions: pool, factory: factory)
        }
    }

    let logger: Logger
    let poolInstruments: PoolInstruments?
    private let lastStatisticsID = NIOLockedValueBox(0)

    init(logger: Logger, metricsFactory: (any MetricsFactory)?) {
        self.logger = logger
        self.poolInstruments = metricsFactory.map {
            PoolInstruments(factory: $0, poolName: "postgresnio-connection-pool")
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
            instruments.maxConnections.record(statistics.maxConnections)
            instruments.maxIdleConnections.record(statistics.maxIdleConnections)
            instruments.minIdleConnections.record(statistics.minIdleConnections)
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
