
public struct ConnectionRequest<Connection: PooledConnection>: ConnectionRequestProtocol {
    public typealias ID = Int

    public var id: ID

    private var continuation: CheckedContinuation<Connection, ConnectionPoolError>

    init(
        id: Int,
        continuation: CheckedContinuation<Connection, ConnectionPoolError>
    ) {
        self.id = id
        self.continuation = continuation
    }

    public func complete(with result: Result<Connection, ConnectionPoolError>) {
        self.continuation.resume(with: result)
    }
}
