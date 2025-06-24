#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

@usableFromInline
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PoolConfiguration: Sendable {
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
>: Sendable where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID {

    @usableFromInline
    struct ConnectionRequest: Hashable, Sendable {
        @usableFromInline var connectionID: ConnectionID

        @inlinable
        init(connectionID: ConnectionID) {
            self.connectionID = connectionID
        }
    }

    @usableFromInline
    struct Action {
        @usableFromInline let request: RequestAction
        @usableFromInline let connection: ConnectionAction

        @inlinable
        init(request: RequestAction, connection: ConnectionAction) {
            self.request = request
            self.connection = connection
        }

        @inlinable
        static func none() -> Action { Action(request: .none, connection: .none) }
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
        case makeConnection(ConnectionRequest, TinyFastSequence<TimerCancellationToken>)
        case runKeepAlive(Connection, TimerCancellationToken?)
        case cancelTimers(TinyFastSequence<TimerCancellationToken>)
        case closeConnection(Connection, Max2Sequence<TimerCancellationToken>)
        case shutdown(Shutdown)

        case none
    }

    @usableFromInline
    enum RequestAction {
        case leaseConnection(TinyFastSequence<Request>, Connection)

        case failRequest(Request, ConnectionPoolError)
        case failRequests(TinyFastSequence<Request>, ConnectionPoolError)

        case none
    }

    @usableFromInline
    enum PoolState: Sendable {
        case running
        case shuttingDown(graceful: Bool)
        case shutDown
    }

    @usableFromInline
    struct Timer: Hashable, Sendable {
        @usableFromInline
        var underlying: ConnectionTimer

        @usableFromInline
        var duration: Duration

        @inlinable
        var connectionID: ConnectionID {
            self.underlying.connectionID
        }

        @inlinable
        init(_ connectionTimer: ConnectionTimer, duration: Duration) {
            self.underlying = connectionTimer
            self.duration = duration
        }
    }

    @usableFromInline let configuration: PoolConfiguration
    @usableFromInline let generator: ConnectionIDGenerator

    @usableFromInline
    private(set) var connections: ConnectionGroup
    @usableFromInline
    private(set) var requestQueue: RequestQueue
    @usableFromInline
    private(set) var poolState: PoolState = .running
    @usableFromInline
    private(set) var cacheNoMoreConnectionsAllowed: Bool = false

    @usableFromInline
    private(set) var failedConsecutiveConnectionAttempts: Int = 0
    
    @inlinable
    init(
        configuration: PoolConfiguration,
        generator: ConnectionIDGenerator,
        timerCancellationTokenType: TimerCancellationToken.Type
    ) {
        self.configuration = configuration
        self.generator = generator
        self.connections = ConnectionGroup(
            generator: generator,
            minimumConcurrentConnections: configuration.minimumConnectionCount,
            maximumConcurrentConnectionSoftLimit: configuration.maximumConnectionSoftLimit,
            maximumConcurrentConnectionHardLimit: configuration.maximumConnectionHardLimit,
            keepAlive: configuration.keepAliveDuration != nil,
            keepAliveReducesAvailableStreams: true
        )
        self.requestQueue = RequestQueue()
    }

    mutating func refillConnections() -> [ConnectionRequest] {
        return self.connections.refillConnections()
    }

    @inlinable
    mutating func leaseConnection(_ request: Request) -> Action {
        switch self.poolState {
        case .running:
            break

        case .shuttingDown, .shutDown:
            return .init(
                request: .failRequest(request, ConnectionPoolError.poolShutdown),
                connection: .none
            )
        }

        if !self.requestQueue.isEmpty && self.cacheNoMoreConnectionsAllowed {
            self.requestQueue.queue(request)
            return .none()
        }

        var soonAvailable: UInt16 = 0

        // check if any other EL has an idle connection
        switch self.connections.leaseConnectionOrSoonAvailableConnectionCount() {
        case .leasedConnection(let leaseResult):
            return .init(
                request: .leaseConnection(TinyFastSequence(element: request), leaseResult.connection),
                connection: .cancelTimers(.init(leaseResult.timersToCancel))
            )

        case .startingCount(let count):
            soonAvailable += count
        }

        // we tried everything. there is no connection available. now we must check, if and where we
        // can create further connections. but first we must enqueue the new request

        self.requestQueue.queue(request)

        let requestAction = RequestAction.none

        if soonAvailable >= self.requestQueue.count {
            // if more connections will be soon available then we have waiters, we don't need to
            // create further new connections.
            return .init(
                request: requestAction,
                connection: .none
            )
        } else if let request = self.connections.createNewDemandConnectionIfPossible() {
            // Can we create a demand connection
            return .init(
                request: requestAction,
                connection: .makeConnection(request, .init())
            )
        } else if let request = self.connections.createNewOverflowConnectionIfPossible() {
            // Can we create an overflow connection
            return .init(
                request: requestAction,
                connection: .makeConnection(request, .init())
            )
        } else {
            self.cacheNoMoreConnectionsAllowed = true

            // no new connections allowed:
            return .init(request: requestAction, connection: .none)
        }
    }

    @inlinable
    mutating func releaseConnection(_ connection: Connection, streams: UInt16) -> Action {
        guard let (index, context) = self.connections.releaseConnection(connection.id, streams: streams) else {
            return .none()
        }
        return self.handleAvailableConnection(index: index, availableContext: context)
    }

    mutating func cancelRequest(id: RequestID) -> Action {
        guard let request = self.requestQueue.remove(id) else {
            return .none()
        }

        return .init(
            request: .failRequest(request, ConnectionPoolError.requestCancelled),
            connection: .none
        )
    }

    @inlinable
    mutating func connectionEstablished(_ connection: Connection, maxStreams: UInt16) -> Action {
        switch self.poolState {
        case .running, .shuttingDown(graceful: true):
            let (index, context) = self.connections.newConnectionEstablished(connection, maxStreams: maxStreams)
            return self.handleAvailableConnection(index: index, availableContext: context)
        case .shuttingDown(graceful: false), .shutDown:
            return .init(request: .none, connection: .closeConnection(connection, []))
        }
    }

    @inlinable
    mutating func connectionReceivedNewMaxStreamSetting(
        _ connection: ConnectionID,
        newMaxStreamSetting maxStreams: UInt16
    ) -> Action {
        guard let info = self.connections.connectionReceivedNewMaxStreamSetting(connection, newMaxStreamSetting: maxStreams) else {
            return .none()
        }

        let waitingRequests = self.requestQueue.count

        guard waitingRequests > 0 else {
            return .none()
        }

        // the only thing we can do if we receive a new max stream setting is check if the new stream
        // setting is higher and then dequeue some waiting requests

        guard info.newMaxStreams > info.oldMaxStreams && info.newMaxStreams > info.usedStreams else {
            return .none()
        }

        let leaseStreams = min(info.newMaxStreams - info.oldMaxStreams, info.newMaxStreams - info.usedStreams, UInt16(clamping: waitingRequests))
        let requests = self.requestQueue.pop(max: leaseStreams)
        precondition(Int(leaseStreams) == requests.count)
        let leaseResult = self.connections.leaseConnection(at: info.index, streams: leaseStreams)

        return .init(
            request: .leaseConnection(requests, leaseResult.connection),
            connection: .cancelTimers(.init(leaseResult.timersToCancel))
        )
    }

    @inlinable
    mutating func timerScheduled(_ timer: Timer, cancelContinuation: TimerCancellationToken) -> TimerCancellationToken? {
        self.connections.timerScheduled(timer.underlying, cancelContinuation: cancelContinuation)
    }

    @inlinable
    mutating func timerTriggered(_ timer: Timer) -> Action {
        switch timer.underlying.usecase {
        case .backoff:
            return self.connectionCreationBackoffDone(timer.connectionID)
        case .keepAlive:
            return self.connectionKeepAliveTimerTriggered(timer.connectionID)
        case .idleTimeout:
            return self.connectionIdleTimerTriggered(timer.connectionID)
        }
    }

    @inlinable
    mutating func connectionEstablishFailed(_ error: Error, for request: ConnectionRequest) -> Action {
        switch self.poolState {
        case .running, .shuttingDown(graceful: true):
            self.failedConsecutiveConnectionAttempts += 1

            let connectionTimer = self.connections.backoffNextConnectionAttempt(request.connectionID)
            let backoff = Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
            let timer = Timer(connectionTimer, duration: backoff)
            return .init(request: .none, connection: .scheduleTimers(.init(timer)))

        case .shuttingDown(graceful: false), .shutDown:
            return .none()
        }
    }

    @inlinable
    mutating func connectionCreationBackoffDone(_ connectionID: ConnectionID) -> Action {
        switch self.poolState {
        case .running, .shuttingDown(graceful: true):
            let soonAvailable = self.connections.soonAvailableConnections
            let retry = (soonAvailable - 1) < self.requestQueue.count

            switch self.connections.backoffDone(connectionID, retry: retry) {
            case .createConnection(let request, let continuation):
                let timers: TinyFastSequence<TimerCancellationToken>
                if let continuation {
                    timers = .init(element: continuation)
                } else {
                    timers = .init()
                }
                return .init(request: .none, connection: .makeConnection(request, timers))

            case .cancelTimers(let timers):
                return .init(request: .none, connection: .cancelTimers(.init(timers)))
            }

        case .shuttingDown(graceful: false), .shutDown:
            return .none()
        }
    }

    @inlinable
    mutating func connectionKeepAliveTimerTriggered(_ connectionID: ConnectionID) -> Action {
        precondition(self.configuration.keepAliveDuration != nil)
        precondition(self.requestQueue.isEmpty)

        guard let keepAliveAction = self.connections.keepAliveIfIdle(connectionID) else {
            return .none()
        }
        return .init(request: .none, connection: .runKeepAlive(keepAliveAction.connection, keepAliveAction.keepAliveTimerCancellationContinuation))
    }

    @inlinable
    mutating func connectionKeepAliveDone(_ connection: Connection) -> Action {
        precondition(self.configuration.keepAliveDuration != nil)
        guard let (index, context) = self.connections.keepAliveSucceeded(connection.id) else {
            return .none()
        }
        return self.handleAvailableConnection(index: index, availableContext: context)
    }

    @inlinable
    mutating func connectionKeepAliveFailed(_ connectionID: ConnectionID) -> Action {
        guard let closeAction = self.connections.keepAliveFailed(connectionID) else {
            return .none()
        }

        return .init(request: .none, connection: .closeConnection(closeAction.connection, closeAction.timersToCancel))
    }

    @inlinable
    mutating func connectionIdleTimerTriggered(_ connectionID: ConnectionID) -> Action {
        precondition(self.requestQueue.isEmpty)

        guard let closeAction = self.connections.closeConnectionIfIdle(connectionID) else {
            return .none()
        }

        self.cacheNoMoreConnectionsAllowed = false
        return .init(request: .none, connection: .closeConnection(closeAction.connection, closeAction.timersToCancel))
    }

    @inlinable
    mutating func connectionClosed(_ connection: Connection) -> Action {
        switch self.poolState {
        case .running, .shuttingDown(graceful: true):
            self.cacheNoMoreConnectionsAllowed = false

            let closedConnectionAction = self.connections.connectionClosed(connection.id)

            let connectionAction: ConnectionAction
            if let newRequest = closedConnectionAction.newConnectionRequest {
                connectionAction = .makeConnection(newRequest, closedConnectionAction.timersToCancel)
            } else {
                connectionAction = .cancelTimers(closedConnectionAction.timersToCancel)
            }

            return .init(request: .none, connection: connectionAction)

        case .shuttingDown(graceful: false), .shutDown:
            return .none()
        }
    }

    struct CleanupAction {
        struct ConnectionToDrop {
            var connection: Connection
            var keepAliveTimer: Bool
            var idleTimer: Bool
        }

        var connections: [ConnectionToDrop]
        var requests: [Request]
    }

    mutating func triggerGracefulShutdown() -> Action {
        fatalError("Unimplemented")
    }

    @usableFromInline
    mutating func triggerForceShutdown() -> Action {
        switch self.poolState {
        case .running:
            self.poolState = .shuttingDown(graceful: false)
            var shutdown = ConnectionAction.Shutdown()
            self.connections.triggerForceShutdown(&shutdown)

            if shutdown.connections.isEmpty {
                self.poolState = .shutDown
            }

            return .init(
                request: .failRequests(self.requestQueue.removeAll(), ConnectionPoolError.poolShutdown),
                connection: .shutdown(shutdown)
            )

        case .shuttingDown:
            return .none()

        case .shutDown:
            return .init(request: .none, connection: .none)
        }
    }

    @inlinable
    /*private*/ mutating func handleAvailableConnection(
        index: Int,
        availableContext: ConnectionGroup.AvailableConnectionContext
    ) -> Action {
        // this connection was busy before
        let requests = self.requestQueue.pop(max: availableContext.info.availableStreams)
        if !requests.isEmpty {
            let leaseResult = self.connections.leaseConnection(at: index, streams: UInt16(requests.count))
            return .init(
                request: .leaseConnection(requests, leaseResult.connection),
                connection: .cancelTimers(.init(leaseResult.timersToCancel))
            )
        }

        switch availableContext.use {
        case .persisted, .demand:
            switch availableContext.info {
            case .leased:
                return .none()

            case .idle(_, let newIdle):
                let timers = self.connections.parkConnection(at: index, hasBecomeIdle: newIdle).map(self.mapTimers)

                return .init(
                    request: .none,
                    connection: .scheduleTimers(timers)
                )
            }

        case .overflow:
            if let closeAction = self.connections.closeConnectionIfIdle(at: index) {
                return .init(
                    request: .none,
                    connection: .closeConnection(closeAction.connection, closeAction.timersToCancel)
                )
            } else {
                return .none()
            }
        }

    }

    @inlinable
    /* private */ func mapTimers(_ connectionTimer: ConnectionTimer) -> Timer {
        switch connectionTimer.usecase {
        case .backoff:
            return Timer(
                connectionTimer,
                duration: Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
            )

        case .keepAlive:
            return Timer(connectionTimer, duration: self.configuration.keepAliveDuration!)

        case .idleTimeout:
            return Timer(connectionTimer, duration: self.configuration.idleTimeoutDuration)

        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine {
    /// Calculates the delay for the next connection attempt after the given number of failed `attempts`.
    ///
    /// Our backoff formula is: 100ms * 1.25^(attempts - 1) with 3% jitter that is capped of at 1 minute.
    /// This means for:
    ///   -  1 failed attempt :  100ms
    ///   -  5 failed attempts: ~300ms
    ///   - 10 failed attempts: ~930ms
    ///   - 15 failed attempts: ~2.84s
    ///   - 20 failed attempts: ~8.67s
    ///   - 25 failed attempts: ~26s
    ///   - 29 failed attempts: ~60s (max out)
    ///
    /// - Parameter attempts: number of failed attempts in a row
    /// - Returns: time to wait until trying to establishing a new connection
    @usableFromInline
    static func calculateBackoff(failedAttempt attempts: Int) -> Duration {
        // Our backoff formula is: 100ms * 1.25^(attempts - 1) that is capped of at 1minute
        // This means for:
        //   -  1 failed attempt :  100ms
        //   -  5 failed attempts: ~300ms
        //   - 10 failed attempts: ~930ms
        //   - 15 failed attempts: ~2.84s
        //   - 20 failed attempts: ~8.67s
        //   - 25 failed attempts: ~26s
        //   - 29 failed attempts: ~60s (max out)

        let start = Double(100_000_000)
        let backoffNanosecondsDouble = start * pow(1.25, Double(attempts - 1))

        // Cap to 60s _before_ we convert to Int64, to avoid trapping in the Int64 initializer.
        let backoffNanoseconds = Int64(min(backoffNanosecondsDouble, Double(60_000_000_000)))

        let backoff = Duration.nanoseconds(backoffNanoseconds)

        // Calculate a 3% jitter range
        let jitterRange = (backoffNanoseconds / 100) * 3
        // Pick a random element from the range +/- jitter range.
        let jitter: Duration = .nanoseconds((-jitterRange...jitterRange).randomElement()!)
        let jitteredBackoff = backoff + jitter
        return jitteredBackoff
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.Action: Equatable where TimerCancellationToken: Equatable, Request: Equatable {}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionAction: Equatable where TimerCancellationToken: Equatable {
    @usableFromInline
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.scheduleTimers(let lhs), .scheduleTimers(let rhs)):
            return lhs == rhs
        case (.makeConnection(let lhsRequest, let lhsToken), .makeConnection(let rhsRequest, let rhsToken)):
            return lhsRequest == rhsRequest && lhsToken == rhsToken
        case (.runKeepAlive(let lhsConn, let lhsToken), .runKeepAlive(let rhsConn, let rhsToken)):
            return lhsConn === rhsConn && lhsToken == rhsToken
        case (.closeConnection(let lhsConn, let lhsTimers), .closeConnection(let rhsConn, let rhsTimers)):
            return lhsConn === rhsConn && lhsTimers == rhsTimers
        case (.shutdown(let lhs), .shutdown(let rhs)):
            return lhs == rhs
        case (.cancelTimers(let lhs), .cancelTimers(let rhs)):
            return lhs == rhs
        case (.none, .none),
             (.cancelTimers([]), .none), (.none, .cancelTimers([])),
             (.scheduleTimers([]), .none), (.none, .scheduleTimers([])):
            return true
        default:
            return false
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionAction.Shutdown: Equatable where TimerCancellationToken: Equatable {
    @usableFromInline
    static func ==(lhs: Self, rhs: Self) -> Bool {
        Set(lhs.connections.lazy.map(\.id)) == Set(rhs.connections.lazy.map(\.id)) && lhs.timersToCancel == rhs.timersToCancel
    }
}


@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.RequestAction: Equatable where Request: Equatable {
    
    @usableFromInline
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.leaseConnection(let lhsRequests, let lhsConn), .leaseConnection(let rhsRequests, let rhsConn)):
            guard lhsRequests.count == rhsRequests.count else { return false }
            var lhsIterator = lhsRequests.makeIterator()
            var rhsIterator = rhsRequests.makeIterator()
            while let lhsNext = lhsIterator.next(), let rhsNext = rhsIterator.next() {
                guard lhsNext.id == rhsNext.id else { return false }
            }
            return lhsConn === rhsConn

        case (.failRequest(let lhsRequest, let lhsError), .failRequest(let rhsRequest, let rhsError)):
            return lhsRequest.id == rhsRequest.id && lhsError == rhsError

        case (.failRequests(let lhsRequests, let lhsError), .failRequests(let rhsRequests, let rhsError)):
            return Set(lhsRequests.lazy.map(\.id)) == Set(rhsRequests.lazy.map(\.id)) && lhsError == rhsError

        case (.none, .none):
            return true

        default:
            return false
        }
    }
}
