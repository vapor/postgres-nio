public struct ConnectionLease<Connection: PooledConnection>: Sendable {
    public var connection: Connection

    @usableFromInline
    let _release: @Sendable (Connection) -> ()

    @inlinable
    public init(connection: Connection, release: @escaping @Sendable (Connection) -> Void) {
        self.connection = connection
        self._release = release
    }

    @inlinable
    public func release() {
        self._release(self.connection)
    }
}
