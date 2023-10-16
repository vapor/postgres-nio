
public struct ConnectionPoolError: Error, Hashable {
    enum Base: Error, Hashable {
        case requestCancelled
        case poolShutdown
    }

    private let base: Base

    init(_ base: Base) { self.base = base }

    /// The connection requests got cancelled
    public static let requestCancelled = ConnectionPoolError(.requestCancelled)
    /// The connection requests can't be fulfilled as the pool has already been shutdown
    public static let poolShutdown = ConnectionPoolError(.poolShutdown)
}
