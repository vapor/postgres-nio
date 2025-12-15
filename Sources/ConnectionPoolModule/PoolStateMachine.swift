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
    var circuitBreakerTripAfter: Duration = .seconds(15)

    @usableFromInline
    var idleTimeoutDuration: Duration = .seconds(30)

    @usableFromInline
    var maximumConnectionRequestsAtOneTime: Int = 20
}

@usableFromInline
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PoolStateMachine<
    Connection: PooledConnection,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    ConnectionID: Hashable & Sendable,
    Request: ConnectionRequestProtocol,
    RequestID,
    TimerCancellationToken: Sendable,
    Clock: _Concurrency.Clock,
    Instant: InstantProtocol
>: Sendable where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID, Clock.Duration == Duration, Clock.Instant == Instant {

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
        case makeConnectionsCancelAndScheduleTimers(TinyFastSequence<ConnectionRequest>, TinyFastSequence<TimerCancellationToken>, Max2Sequence<Timer>)
        case runKeepAlive(Connection, TimerCancellationToken?)
        case cancelTimers(TinyFastSequence<TimerCancellationToken>)
        case closeConnection(Connection, Max2Sequence<TimerCancellationToken>)
        /// Start process of shutting down the connection pool. Close connections, cancel timers.
        case initiateShutdown(Shutdown)
        /// All connections have been closed, the pool event stream can be ended. 
        case cancelEventStreamAndFinalCleanup([TimerCancellationToken])
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
        @usableFromInline
        struct ConnectionCreationFailingContext: Sendable {
            @usableFromInline
            init(
                timeOfFirstFailedAttempt: Clock.Instant, 
                error: any Error, 
                connectionIDToRetry: ConnectionID
            ) {
                self.timeOfFirstFailedAttempt = timeOfFirstFailedAttempt
                self.firstError = error
                self.lastError = error
                self.numberOfFailedAttempts = 1
                self.connectionIDToRetry = connectionIDToRetry
            }

            @usableFromInline
            var timeOfFirstFailedAttempt: Clock.Instant
            @usableFromInline
            var firstError: any Error
            @usableFromInline
            var lastError: any Error
            @usableFromInline
            var numberOfFailedAttempts: Int
            @usableFromInline
            var connectionIDToRetry: ConnectionID
        }

        @usableFromInline
        struct CircuitBreakerOpenContext: Sendable {
            @usableFromInline
            init(_ creationFailingContext: ConnectionCreationFailingContext) {

                self.firstError = creationFailingContext.firstError
                self.lastError = creationFailingContext.lastError
                self.numberOfFailedAttempts = creationFailingContext.numberOfFailedAttempts
                self.connectionIDToRetry = creationFailingContext.connectionIDToRetry
            }

            @usableFromInline
            var firstError: any Error
            @usableFromInline
            var lastError: any Error
            @usableFromInline
            var numberOfFailedAttempts: Int
            @usableFromInline
            var connectionIDToRetry: ConnectionID
        }

        /// Everything is awesome. Connections are created as they are needed.
        /// Can transition to:
        ///   - `shuttingDown` if the pool is being shut down (graceful shutdown behavior is managed by an external flag),
        ///   - `connectionCreationFailing` if a connection creation failed.
        case running
        /// The last connection creation attempt failed. In this state, the pool attempts to establish 
        /// only one connection to the server at a time. New connection attempts are not initiated based 
        /// on incoming requests. Retries to establish a connection continue even if all requests have 
        /// finished. Existing connections continue to serve requests.
        /// Can transition to:
        ///   - `circuitBreakOpen` on failed connection if the timer passed since entering this state has passed
        ///     connectionTimeout AND there are zero open connections. The pool remains in `connectionCreationFailing`
        ///     until the last open connection is closed.
        ///   - `running` if a new connection can be successfully established.
        ///   - `shuttingDown` if the pool is shut down.
        case connectionCreationFailing(ConnectionCreationFailingContext)
        /// The circuit breaker has tripped. This state is entered from `connectionCreationFailing`
        /// when the circuit breaker timer has elapsed AND there are zero open connections.
        /// Upon entering this state, all currently queued requests are failed, and any new incoming
        /// requests are immediately rejected. The pool will periodically attempt to establish a new
        /// connection after a backoff period.
        /// Can transition to:
        ///   - `running` if a new connection can be successfully established.
        ///   - `shuttingDown` if the pool is shut down.
        case circuitBreakOpen(CircuitBreakerOpenContext)

        /// The pool is in the process of shutting down. Graceful shutdown behavior (e.g., waiting for
        /// in-flight requests to complete) is managed by an external `gracefulShutdownTriggered` flag,
        /// rather than being part of the state itself.
        /// Can transition to:
        ///   - `shutDown` once all resources are released and outstanding requests are handled (if graceful shutdown was triggered).
        case shuttingDown
        /// The pool has fully shut down and released all its resources. No further operations are possible.
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
    private(set) var gracefulShutdownTriggered: Bool = false
    @usableFromInline
    let clock: Clock
    @usableFromInline
    private(set) var cacheNoMoreConnectionsAllowed: Bool = false

    @inlinable
    init(
        configuration: PoolConfiguration,
        generator: ConnectionIDGenerator,
        timerCancellationTokenType: TimerCancellationToken.Type,
        clock: Clock
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
        self.clock = clock
        self.requestQueue = RequestQueue()
    }

    mutating func refillConnections() -> [ConnectionRequest] {
        return self.connections.refillConnections()
    }

    @inlinable
    mutating func leaseConnection(_ request: Request) -> Action {
        switch self.poolState {
        case .running:
            // if requestQueue is non-empty and we cannot create more connections add
            // to queue and do nothing otherwise fallthrough to the rest of the function
            if !self.requestQueue.isEmpty && self.cacheNoMoreConnectionsAllowed {
                self.requestQueue.queue(request)
                return .none()
            }
        case .connectionCreationFailing:
            self.requestQueue.queue(request)
            return .none()

        case .circuitBreakOpen:
            return .init(
                request: .failRequest(request, ConnectionPoolError.connectionCreationCircuitBreakerTripped),
                connection: .none
            )

        case .shuttingDown, .shutDown:
            return .init(
                request: .failRequest(request, ConnectionPoolError.poolShutdown),
                connection: .none
            )
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
        case .running:
            break

        case .shuttingDown:
            break

        case .connectionCreationFailing, .circuitBreakOpen:
            self.poolState = .running

        case .shutDown:
            fatalError("Connection pool is not running")
        }

        let (index, context) = self.connections.newConnectionEstablished(connection, maxStreams: maxStreams)
        return self.handleAvailableConnection(index: index, availableContext: context)
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
        case .running:
            self.poolState = .connectionCreationFailing(
                .init(
                    timeOfFirstFailedAttempt: clock.now, 
                    error: error, 
                    connectionIDToRetry: request.connectionID
                )
            )
            let timer = self.backoffNextConnectionAttempt(connectionID: request.connectionID, numberOfFailedAttempts: 1)
            return .init(request: .none, connection: .scheduleTimers(.init(timer)))

        case .connectionCreationFailing(var creationFailingContext):
            guard request.connectionID == creationFailingContext.connectionIDToRetry else {
                let timers = self.connections.destroyFailedConnection(request.connectionID)
                return .init(request: .none, connection: .cancelTimers(timers.map { [$0] } ?? []))
            }
            creationFailingContext.lastError = error
            creationFailingContext.numberOfFailedAttempts += 1
            var requestAction: RequestAction = .none
            // if failing for longer than connection timeout and there are no open connections move to circuit break state
            if creationFailingContext.timeOfFirstFailedAttempt.duration(to: clock.now) > self.configuration.circuitBreakerTripAfter, 
                self.connections.stats.idle + self.connections.stats.leased == 0 {
                self.poolState = .circuitBreakOpen(.init(creationFailingContext))
                requestAction = .failRequests(self.requestQueue.removeAll(), ConnectionPoolError.connectionCreationCircuitBreakerTripped)
            } else {
                self.poolState = .connectionCreationFailing(creationFailingContext)
            }
            let timer = self.backoffNextConnectionAttempt(
                connectionID: request.connectionID, 
                numberOfFailedAttempts: creationFailingContext.numberOfFailedAttempts
            )
            return .init(request: requestAction, connection: .scheduleTimers(.init(timer)))
            
        case .circuitBreakOpen(var circuitBreakOpenContext):
            guard request.connectionID == circuitBreakOpenContext.connectionIDToRetry else {
                let timers = self.connections.destroyFailedConnection(request.connectionID)
                return .init(request: .none, connection: .cancelTimers(timers.map { [$0] } ?? []))
            }
            circuitBreakOpenContext.lastError = error
            circuitBreakOpenContext.numberOfFailedAttempts += 1
            self.poolState = .circuitBreakOpen(circuitBreakOpenContext)
            let timer = self.backoffNextConnectionAttempt(
                connectionID: request.connectionID, 
                numberOfFailedAttempts: circuitBreakOpenContext.numberOfFailedAttempts
            )
            return .init(request: .none, connection: .scheduleTimers(.init(timer)))
            
        case .shuttingDown, .shutDown:
            let timerToCancel = self.connections.destroyFailedConnection(request.connectionID)
            let connectionAction: ConnectionAction
            if self.connections.isEmpty {
                self.poolState = .shutDown
                connectionAction = .cancelEventStreamAndFinalCleanup(timerToCancel.map {[$0]} ?? [])
            } else {
                connectionAction = .cancelTimers(timerToCancel.map {[$0]} ?? [])
            }
            return .init(
                request: .none,
                connection: connectionAction
            )
        }
    }

    @inlinable
    mutating func backoffNextConnectionAttempt(connectionID: ConnectionID, numberOfFailedAttempts: Int) -> Timer {
        let connectionTimer = self.connections.backoffNextConnectionAttempt(connectionID)
        let backoff = Self.calculateBackoff(failedAttempt: numberOfFailedAttempts)
        return Timer(connectionTimer, duration: backoff)
    }

    @inlinable
    mutating func connectionCreationBackoffDone(_ connectionID: ConnectionID) -> Action {
        switch self.poolState {
        case .connectionCreationFailing(let context):
            // if connection id is not the same as retrying connection id destroy connection
            // otherwise fallthrough to backoffDone code
            guard connectionID == context.connectionIDToRetry else {
                let timers = self.connections.destroyFailedConnection(connectionID)
                return .init(request: .none, connection: .cancelTimers(timers.map { [$0] } ?? []))
            }

        case .circuitBreakOpen(let context):
            // if connection id is not the same as retrying connection id destroy connection
            // otherwise fallthrough to backoffDone code
            guard connectionID == context.connectionIDToRetry else {
                let timers = self.connections.destroyFailedConnection(connectionID)
                return .init(request: .none, connection: .cancelTimers(timers.map { [$0] } ?? []))
            }

        case .running:
            preconditionFailure("Invalid state")

        case .shuttingDown, .shutDown:
            return .none()
        }

        switch self.connections.backoffDone(connectionID, retry: true) {
        case .createConnection(let request, let continuation):
            let timers: TinyFastSequence<TimerCancellationToken>
            if let continuation {
                timers = .init(element: continuation)
            } else {
                timers = .init()
            }
            return .init(request: .none, connection: .makeConnection(request, timers))

        case .cancelTimers(let timers):
            let connectionAction: ConnectionAction
            if self.connections.isEmpty {
                self.poolState = .shutDown
                connectionAction = .cancelEventStreamAndFinalCleanup(.init(timers))
            } else {
                connectionAction = .cancelTimers(.init(timers))
            }
            return .init(
                request: .none,
                connection: connectionAction
            )
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
        case .running, .connectionCreationFailing, .circuitBreakOpen:
            self.cacheNoMoreConnectionsAllowed = false

            let closedConnectionAction = self.connections.connectionClosed(connection.id, shuttingDown: self.gracefulShutdownTriggered)

            let connectionAction: ConnectionAction
            if let newRequest = closedConnectionAction.newConnectionRequest {
                connectionAction = .makeConnection(newRequest, closedConnectionAction.timersToCancel)
            } else {
                connectionAction = .cancelTimers(closedConnectionAction.timersToCancel)
            }

            return .init(request: .none, connection: connectionAction)

        case .shuttingDown:
            let closedConnectionAction = self.connections.connectionClosed(connection.id, shuttingDown: true)

            let connectionAction: ConnectionAction
            if self.connections.isEmpty {
                self.poolState = .shutDown
                connectionAction = .cancelEventStreamAndFinalCleanup(.init(closedConnectionAction.timersToCancel))
            } else {
                connectionAction = .cancelTimers(closedConnectionAction.timersToCancel)
            }
            return .init(request: .none, connection: connectionAction)

        case .shutDown:
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
        case .running, .connectionCreationFailing, .circuitBreakOpen:
            self.poolState = .shuttingDown
            var shutdown = ConnectionAction.Shutdown()
            self.connections.triggerForceShutdown(&shutdown)

            if self.connections.isEmpty, shutdown.connections.isEmpty {
                self.poolState = .shutDown
                return .init(
                    request: .failRequests(self.requestQueue.removeAll(), ConnectionPoolError.poolShutdown),
                    connection: .cancelEventStreamAndFinalCleanup(shutdown.timersToCancel)
                )
            }

            return .init(
                request: .failRequests(self.requestQueue.removeAll(), ConnectionPoolError.poolShutdown),
                connection: .initiateShutdown(shutdown)
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
            let connectionsRequired = self.configuration.minimumConnectionCount - Int(self.connections.stats.active)
            let connectionAction = self.createMultipleConnectionsAction(
                connectionsRequired, 
                cancelledTimers: .init(leaseResult.timersToCancel), 
                scheduledTimers: []
            ) ?? .cancelTimers(.init(leaseResult.timersToCancel))
            return .init(
                request: .leaseConnection(requests, leaseResult.connection),
                connection: connectionAction
            )
        }

        switch availableContext.use {
        case .persisted, .demand:
            switch availableContext.info {
            case .leased:
                return .none()

            case .idle(_, let newIdle):
                if case .shuttingDown = self.poolState {
                    switch self.connections.closeConnection(at: index, deleteConnection: true) {
                    case .close(let closeAction):
                        return .init(
                            request: .none,
                            connection: .closeConnection(closeAction.connection, closeAction.timersToCancel)
                        )
                    case .cancelTimers(let timers):
                        return .init(
                            request: .none,
                            connection: .cancelTimers(.init(timers))
                        )
                    case .doNothing:
                        return .none()
                    }
                }
                let timers = self.connections.parkConnection(at: index, hasBecomeIdle: newIdle).map(self.mapTimers)

                let connectionsRequired = self.configuration.minimumConnectionCount - Int(self.connections.stats.active)
                let connectionAction = self.createMultipleConnectionsAction(
                    connectionsRequired, 
                    cancelledTimers: [], 
                    scheduledTimers: timers
                ) ?? .scheduleTimers(timers)
                return .init(request: .none, connection: connectionAction)
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
    /* private */ mutating func createMultipleConnectionsAction(
        _ connectionCount: Int, 
        cancelledTimers: TinyFastSequence<TimerCancellationToken>, 
        scheduledTimers: Max2Sequence<Timer>
    ) -> ConnectionAction? {
        if connectionCount > 0, 
            self.connections.stats.connecting < self.configuration.maximumConnectionRequestsAtOneTime {
            let connectionCount = min(
                connectionCount, 
                self.configuration.maximumConnectionRequestsAtOneTime - Int(self.connections.stats.connecting)
            )
            var connectionRequests = TinyFastSequence<ConnectionRequest>()
            connectionRequests.reserveCapacity(connectionCount)
            for _ in 0..<connectionCount {
                connectionRequests.append(self.connections.createNewConnection())
            }
            return .makeConnectionsCancelAndScheduleTimers(connectionRequests, cancelledTimers, scheduledTimers)
        }
        return nil
    }

    @inlinable
    /* private */ func mapTimers(_ connectionTimer: ConnectionTimer) -> Timer {
        switch connectionTimer.usecase {
        case .backoff:
            return Timer(
                connectionTimer,
                duration: Self.calculateBackoff(failedAttempt: 1)
            )

        case .keepAlive:
            return Timer(connectionTimer, duration: self.configuration.keepAliveDuration!)

        case .idleTimeout:
            return Timer(connectionTimer, duration: self.configuration.idleTimeoutDuration)

        }
    }

    // Is connection pool shutdown.
    public var isShutdown: Bool { 
        if case .shutDown = self.poolState { return true }
        return false
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
//extension PoolStateMachine.PoolState: Equatable {}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionAction: Equatable where TimerCancellationToken: Equatable {
    @usableFromInline
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.scheduleTimers(let lhs), .scheduleTimers(let rhs)):
            return lhs == rhs
        case (.makeConnection(let lhsRequest, let lhsToken), .makeConnection(let rhsRequest, let rhsToken)):
            return lhsRequest == rhsRequest && lhsToken == rhsToken
        case (.makeConnectionsCancelAndScheduleTimers(let lhsRequests, let lhsTokens, let lhsTimers),
            .makeConnectionsCancelAndScheduleTimers(let rhsRequests, let rhsTokens, let rhsTimers)):
            return lhsRequests == rhsRequests && lhsTokens == rhsTokens && lhsTimers == rhsTimers
        case (.runKeepAlive(let lhsConn, let lhsToken), .runKeepAlive(let rhsConn, let rhsToken)):
            return lhsConn === rhsConn && lhsToken == rhsToken
        case (.closeConnection(let lhsConn, let lhsTimers), .closeConnection(let rhsConn, let rhsTimers)):
            return lhsConn === rhsConn && lhsTimers == rhsTimers
        case (.initiateShutdown(let lhs), .initiateShutdown(let rhs)):
            return lhs == rhs
        case (.cancelEventStreamAndFinalCleanup(let lhs), .cancelEventStreamAndFinalCleanup(let rhs)):
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
