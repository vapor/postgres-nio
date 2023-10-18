#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

@usableFromInline
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes idle connections,
    /// the `ConnectionPool` will initiate new outbound connections proactively
    /// to avoid the number of available connections dropping below this number.
    @usableFromInline
    var minimumConnectionCount: Int = 0

    /// The maximum number of connections to for this pool, to be preserved.
    @usableFromInline
    var maximumConnectionSoftLimit: Int = 10

    @usableFromInline
    var maximumConnectionHardLimit: Int = 10

    @usableFromInline
    var keepAliveDuration: Duration?

    @usableFromInline
    var idleTimeoutDuration: Duration = .seconds(30)
}

@usableFromInline
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PoolStateMachine<
    Connection: PooledConnection,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    ConnectionID: Hashable & Sendable,
    Request: ConnectionRequestProtocol,
    RequestID,
    TimerCancellationToken
> where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID {

    @usableFromInline
    struct Timer: Hashable, Sendable {
        @usableFromInline
        enum Usecase: Sendable {
            case backoff
            case idleTimeout
            case keepAlive
        }

        @usableFromInline
        var connectionID: ConnectionID

        @usableFromInline
        var timerID: Int

        @usableFromInline
        var duration: Duration

        @usableFromInline
        var usecase: Usecase

        @inlinable
        init(connectionID: ConnectionID, timerID: Int, duration: Duration, usecase: Usecase) {
            self.connectionID = connectionID
            self.timerID = timerID
            self.duration = duration
            self.usecase = usecase
        }
    }


}
