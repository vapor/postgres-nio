import _ConnectionPoolModule
import Logging

final class PostgresClientMetrics: ConnectionPoolObservabilityDelegate {
    typealias ConnectionID = PostgresConnection.ID

    let logger: Logger

    init(logger: Logger) {
        self.logger = logger
    }

    func startedConnecting(id: ConnectionID) {
        self.logger.debug("Creating new connection", metadata: [
            .connectionID: "\(id)",
        ])
    }

    /// A connection attempt failed with the given error. After some period of
    /// time ``startedConnecting(id:)`` may be called again.
    func connectFailed(id: ConnectionID, error: Error) {
        self.logger.debug("Connection creation failed", metadata: [
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

    func keepAliveFailed(id: PostgresConnection.ID, error: Error) {}

    /// The remote peer is quiescing the connection: no new streams will be created on it. The
    /// connection will eventually be closed and removed from the pool.
    func connectionClosing(id: ConnectionID) {
        self.logger.debug("Close connection", metadata: [
            .connectionID: "\(id)"
        ])
    }

    /// The connection was closed. The connection may be established again in the future (notified
    /// via ``startedConnecting(id:)``).
    func connectionClosed(id: ConnectionID, error: Error?) {
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
