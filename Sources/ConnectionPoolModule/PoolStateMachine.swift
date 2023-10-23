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
    TimerCancellationToken: Sendable
> where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID {
    
    @usableFromInline
    struct ConnectionRequest: Equatable {
        @usableFromInline var connectionID: ConnectionID

        @inlinable
        init(connectionID: ConnectionID) {
            self.connectionID = connectionID
        }
    }

    @usableFromInline
    enum ConnectionAction {
        @usableFromInline
        struct Shutdown {
            @usableFromInline
            var connections: [Connection]
            @usableFromInline
            var timersToCancel: [TimerCancellationToken]

            @inlinable
            init() {
                self.connections = []
                self.timersToCancel = []
            }
        }

        case scheduleTimers(Max2Sequence<Timer>)
        case makeConnection(ConnectionRequest, TimerCancellationToken?)
        case runKeepAlive(Connection, TimerCancellationToken?)
        case cancelTimers(Max2Sequence<TimerCancellationToken>)
        case closeConnection(Connection)
        case shutdown(Shutdown)

        case none
    }

    @usableFromInline
    struct Timer: Hashable, Sendable {
        @usableFromInline
        var underlying: ConnectionTimer

        @usableFromInline
        var duration: Duration

        @inlinable
        init(_ connectionTimer: ConnectionTimer, duration: Duration) {
            self.underlying = connectionTimer
            self.duration = duration
        }
    }
}
