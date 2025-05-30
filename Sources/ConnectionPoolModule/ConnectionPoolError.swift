
public struct ConnectionPoolError: Error, Hashable {
    @usableFromInline
    enum Base: Error, Hashable, Sendable {
        case requestCancelled
        case poolShutdown
    }

    @usableFromInline
    let base: Base

    @inlinable
    init(_ base: Base) { self.base = base }

    /// The connection requests got cancelled
    public static var requestCancelled: Self {
        ConnectionPoolError(.requestCancelled)
    }
    /// The connection requests can't be fulfilled as the pool has already been shutdown
    public static var poolShutdown: Self {
        ConnectionPoolError(.poolShutdown)
    }
}
