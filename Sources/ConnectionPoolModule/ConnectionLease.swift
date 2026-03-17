public struct ConnectionLease<Connection: PooledConnection>: Sendable {
    public var connection: Connection

    public var connectionID: Int

    @usableFromInline
    let _release: @Sendable (ConnectionID) -> ()

    @inlinable
    package init(connection: Connection, connectionID: ConnectionID, release: @escaping @Sendable (ConnectionID) -> Void) {
        self.connection = connection
        self.connectionID = connectionID
        self._release = release
    }

    @inlinable
    public func release() {
        self._release(self.connectionID)
    }
}
