
public struct ConnectionPoolError: Error, Hashable {
    @usableFromInline
    enum Base: Error, Hashable, Sendable {
        case requestCancelled
        case poolShutdown
        case connectionCreationCircuitBreakerTripped
    }

    @usableFromInline
    let base: Base

    @inlinable
    init(_ base: Base) { self.base = base }

    /// The connection request was cancelled.
    @inlinable
    public static var requestCancelled: Self {
        ConnectionPoolError(.requestCancelled)
    }
    /// The connection request can't be fulfilled because the pool has already been shut down.
    @inlinable
    public static var poolShutdown: Self {
        ConnectionPoolError(.poolShutdown)
    }
    /// The connection pool has failed to make a connection after a defined time.
    @inlinable
    public static var connectionCreationCircuitBreakerTripped: Self {
        ConnectionPoolError(.connectionCreationCircuitBreakerTripped)
    }
}
