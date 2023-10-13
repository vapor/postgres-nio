@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct NoOpKeepAliveBehavior<Connection: PooledConnection>: ConnectionKeepAliveBehavior {
    public var keepAliveFrequency: Duration? { nil }

    public func runKeepAlive(for connection: Connection) async throws {}

    public init(connectionType: Connection.Type) {}
}
