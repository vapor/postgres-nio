
public struct ConnectionRequest<Connection: PooledConnection>: ConnectionRequestProtocol {
    public typealias ID = Int

    public var id: ID

    @usableFromInline
    private(set) var continuation: CheckedContinuation<Connection, any Error>

    @inlinable
    init(
        id: Int,
        continuation: CheckedContinuation<Connection, any Error>
    ) {
        self.id = id
        self.continuation = continuation
    }

    public func complete(with result: Result<Connection, ConnectionPoolError>) {
        self.continuation.resume(with: result)
    }
}
