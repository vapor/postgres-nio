
public protocol ConnectionPoolObservabilityDelegate: Sendable {
    associatedtype ConnectionID: Hashable & Sendable

    /// The connection with the given ID has started trying to establish a connection. The outcome
    /// of the connection will be reported as either ``connectSucceeded(id:streamCapacity:)`` or
    /// ``connectFailed(id:error:)``.
    func startedConnecting(id: ConnectionID)

    /// A connection attempt failed with the given error. After some period of
    /// time ``startedConnecting(id:)`` may be called again.
    func connectFailed(id: ConnectionID, error: Error)

    /// A connection was established on the connection with the given ID. `streamCapacity` streams are
    /// available to use on the connection. The maximum number of available streams may change over
    /// time and is reported via ````. The
    func connectSucceeded(id: ConnectionID, streamCapacity: UInt16)

    /// The utlization of the connection changed; a stream may have been used, returned or the
    /// maximum number of concurrent streams available on the connection changed.
    func connectionUtilizationChanged(id:ConnectionID, streamsUsed: UInt16, streamCapacity: UInt16)

    func keepAliveTriggered(id: ConnectionID)

    func keepAliveSucceeded(id: ConnectionID)

    func keepAliveFailed(id: ConnectionID, error: Error)

    /// The remote peer is quiescing the connection: no new streams will be created on it. The
    /// connection will eventually be closed and removed from the pool.
    func connectionClosing(id: ConnectionID)

    /// The connection was closed. The connection may be established again in the future (notified
    /// via ``startedConnecting(id:)``).
    func connectionClosed(id: ConnectionID, error: Error?)

    func requestQueueDepthChanged(_ newDepth: Int)
}

public struct NoOpConnectionPoolMetrics<ConnectionID: Hashable & Sendable>: ConnectionPoolObservabilityDelegate {
    public init(connectionIDType: ConnectionID.Type) {}

    public func startedConnecting(id: ConnectionID) {}

    public func connectFailed(id: ConnectionID, error: Error) {}

    public func connectSucceeded(id: ConnectionID, streamCapacity: UInt16) {}

    public func connectionUtilizationChanged(id: ConnectionID, streamsUsed: UInt16, streamCapacity: UInt16) {}

    public func keepAliveTriggered(id: ConnectionID) {}

    public func keepAliveSucceeded(id: ConnectionID) {}

    public func keepAliveFailed(id: ConnectionID, error: Error) {}

    public func connectionClosing(id: ConnectionID) {}

    public func connectionClosed(id: ConnectionID, error: Error?) {}

    public func requestQueueDepthChanged(_ newDepth: Int) {}
}
