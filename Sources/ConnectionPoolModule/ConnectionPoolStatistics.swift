/// A snapshot of the connection pool's state, taken right after a state change.
public struct ConnectionPoolStatistics: Sendable, Equatable {
    /// Increases with every snapshot. Snapshots can arrive out of order, 
    /// so we should ignore snapshots with an `id` that is lower than the last one processed.
    public let id: Int
    /// Connections that are established and have no leased streams.
    public let idleConnectionCount: Int
    /// Connections that have at least one leased stream.
    public let usedConnectionCount: Int
    /// The maximum number of connections the pool may open.
    public let maxConnections: Int
    /// The number of connections the pool keeps open while idle.
    public let maxIdleConnections: Int
    /// The minimum number of connections the pool keeps open.
    public let minIdleConnections: Int
    /// Requests waiting for a connection.
    public let pendingRequests: Int

    @inlinable
    init(
        id: Int,
        idleConnectionCount: Int,
        usedConnectionCount: Int,
        maxConnections: Int,
        maxIdleConnections: Int,
        minIdleConnections: Int,
        pendingRequests: Int
    ) {
        self.id = id
        self.idleConnectionCount = idleConnectionCount
        self.usedConnectionCount = usedConnectionCount
        self.maxConnections = maxConnections
        self.maxIdleConnections = maxIdleConnections
        self.minIdleConnections = minIdleConnections
        self.pendingRequests = pendingRequests
    }
}
