/// Receives snapshots of the connection pool's state, for example to record them as metrics.
public protocol ConnectionPoolStatisticsReporter {
    /// Called with a snapshot of the pool's state after every operation that changed it, such as a connection
    /// being leased, released, created or closed, or a request being queued.
    ///
    /// Implementations must be thread-safe and should return quickly, as they run on the path of every lease and release.
    /// Calls happen outside the lock so two snapshots can arrive out of order. Always ignore snapshots
    /// whose id is not greater than the last processed one.
    func report(statistics: ConnectionPoolStatistics)
}
