
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ConnectionAndMetadata<Connection: PooledConnection>: Sendable {

    public var connection: Connection

    public var maximalStreamsOnConnection: UInt16

    public init(connection: Connection, maximalStreamsOnConnection: UInt16) {
        self.connection = connection
        self.maximalStreamsOnConnection = maximalStreamsOnConnection
    }
}

/// A connection that can be pooled in a ``ConnectionPool``
public protocol PooledConnection: AnyObject, Sendable {
    /// The connections identifier type.
    associatedtype ID: Hashable & Sendable

    /// The connections identifier. The identifier is passed to
    /// the connection factory method and must stay attached to
    /// the connection at all times. It must not change during
    /// the connections lifetime.
    var id: ID { get }

    /// A method to register closures that are invoked when the
    /// connection is closed. If the connection closed unexpectedly
    /// the closure shall be called with the underlying error.
    /// In most NIO clients this can be easily implemented by
    /// attaching to the `channel.closeFuture`:
    /// ```
    ///   func onClose(
    ///     _ closure: @escaping @Sendable ((any Error)?) -> ()
    ///   ) {
    ///     channel.closeFuture.whenComplete { _ in
    ///       closure(previousError)
    ///     }
    ///   }
    /// ```
    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ())

    /// Close the running connection. Once the close has completed
    /// closures that were registered in `onClose` must be
    /// invoked.
    func close()
}

/// A connection id generator. Its returned connection IDs will
/// be used when creating new ``PooledConnection``s
public protocol ConnectionIDGeneratorProtocol: Sendable {
    /// The connections identifier type.
    associatedtype ID: Hashable & Sendable

    /// The next connection ID that shall be used.
    func next() -> ID
}

/// A keep alive behavior for connections maintained by the pool
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public protocol ConnectionKeepAliveBehavior: Sendable {
    /// the connection type
    associatedtype Connection: PooledConnection

    /// The time after which a keep-alive shall
    /// be triggered.
    /// If nil is returned, keep-alive is deactivated
    var keepAliveFrequency: Duration? { get }

    /// This method is invoked when the keep-alive shall be
    /// run.
    func runKeepAlive(for connection: Connection) async throws
}

/// A request to get a connection from the `ConnectionPool`
public protocol ConnectionRequestProtocol: Sendable {
    /// A connection lease request ID type.
    associatedtype ID: Hashable & Sendable
    /// The leased connection type
    associatedtype Connection: PooledConnection

    /// A connection lease request ID. This ID must be generated
    /// by users of the `ConnectionPool` outside the
    /// `ConnectionPool`. It is not generated inside the pool like
    /// the `ConnectionID`s. The lease request ID must be unique
    /// and must not change, if your implementing type is a
    /// reference type.
    var id: ID { get }

    /// A function that is called with a connection or a
    /// `PoolError`.
    func complete(with: Result<Connection, ConnectionPoolError>)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ConnectionPoolConfiguration: Sendable {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes
    /// idle connections,
    /// the `ConnectionPool` will initiate new outbound
    /// connections proactively to avoid the number of available
    /// connections dropping below this number.
    public var minimumConnectionCount: Int

    /// Between the `minimumConnectionCount` and
    /// `maximumConnectionSoftLimit` the connection pool creates
    /// _preserved_ connections. Preserved connections are closed
    /// if they have been idle for ``idleTimeout``.
    public var maximumConnectionSoftLimit: Int

    /// The maximum number of connections for this pool, that can
    /// exist at any point in time. The pool can create _overflow_
    /// connections, if all connections are leased, and the
    /// `maximumConnectionHardLimit` > `maximumConnectionSoftLimit `
    /// Overflow connections are closed immediately as soon as they
    /// become idle.
    public var maximumConnectionHardLimit: Int

    /// The time that a _preserved_ idle connection stays in the
    /// pool before it is closed.
    public var idleTimeout: Duration

    /// initializer
    public init() {
        self.minimumConnectionCount = 0
        self.maximumConnectionSoftLimit = 16
        self.maximumConnectionHardLimit = 16
        self.idleTimeout = .seconds(60)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class ConnectionPool<
    Connection: PooledConnection,
    ConnectionID: Hashable & Sendable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    Request: ConnectionRequestProtocol,
    RequestID: Hashable & Sendable,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    ObservabilityDelegate: ConnectionPoolObservabilityDelegate,
    Clock: _Concurrency.Clock
>: Sendable where
    Connection.ID == ConnectionID,
    ConnectionIDGenerator.ID == ConnectionID,
    Request.Connection == Connection,
    Request.ID == RequestID,
    KeepAliveBehavior.Connection == Connection,
    ObservabilityDelegate.ConnectionID == ConnectionID,
    Clock.Duration == Duration
{
    public typealias ConnectionFactory = @Sendable (ConnectionID, ConnectionPool<Connection, ConnectionID, ConnectionIDGenerator, Request, RequestID, KeepAliveBehavior, ObservabilityDelegate, Clock>) async throws -> ConnectionAndMetadata<Connection>

    @usableFromInline
    typealias StateMachine = PoolStateMachine<Connection, ConnectionIDGenerator, ConnectionID, Request, Request.ID, CheckedContinuation<Void, Never>>

    @usableFromInline
    let factory: ConnectionFactory

    @usableFromInline
    let keepAliveBehavior: KeepAliveBehavior

    @usableFromInline 
    let observabilityDelegate: ObservabilityDelegate

    @usableFromInline
    let clock: Clock

    @usableFromInline
    let configuration: ConnectionPoolConfiguration

    @usableFromInline
    struct State: Sendable {
        @usableFromInline
        var stateMachine: StateMachine
        @usableFromInline
        var lastConnectError: (any Error)?
    }

    @usableFromInline let stateBox: NIOLockedValueBox<State>

    private let requestIDGenerator = _ConnectionPoolModule.ConnectionIDGenerator()

    @usableFromInline
    let eventStream: AsyncStream<NewPoolActions>

    @usableFromInline
    let eventContinuation: AsyncStream<NewPoolActions>.Continuation

    public init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        requestType: Request.Type,
        keepAliveBehavior: KeepAliveBehavior,
        observabilityDelegate: ObservabilityDelegate,
        clock: Clock,
        connectionFactory: @escaping ConnectionFactory
    ) {
        self.clock = clock
        self.factory = connectionFactory
        self.keepAliveBehavior = keepAliveBehavior
        self.observabilityDelegate = observabilityDelegate
        self.configuration = configuration
        var stateMachine = StateMachine(
            configuration: .init(configuration, keepAliveBehavior: keepAliveBehavior),
            generator: idGenerator,
            timerCancellationTokenType: CheckedContinuation<Void, Never>.self
        )

        let (stream, continuation) = AsyncStream.makeStream(of: NewPoolActions.self)
        self.eventStream = stream
        self.eventContinuation = continuation

        let connectionRequests = stateMachine.refillConnections()

        self.stateBox = NIOLockedValueBox(.init(stateMachine: stateMachine))

        for request in connectionRequests {
            self.eventContinuation.yield(.makeConnection(request))
        }
    }

    @inlinable
    public func releaseConnection(_ connection: Connection, streams: UInt16 = 1) {
        self.modifyStateAndRunActions { state in
            state.stateMachine.releaseConnection(connection, streams: streams)
        }
    }

    @inlinable
    public func leaseConnection(_ request: Request) {
        self.modifyStateAndRunActions { state in
            state.stateMachine.leaseConnection(request)
        }
    }

    @inlinable
    public func leaseConnections(_ requests: some Collection<Request>) {
        let actions = self.stateBox.withLockedValue { state in
            var actions = [StateMachine.Action]()
            actions.reserveCapacity(requests.count)

            for request in requests {
                let stateMachineAction = state.stateMachine.leaseConnection(request)
                actions.append(stateMachineAction)
            }

            return actions
        }

        for action in actions {
            self.runRequestAction(action.request)
            self.runConnectionAction(action.connection)
        }
    }

    public func cancelLeaseConnection(_ requestID: RequestID) {
        self.modifyStateAndRunActions { state in
            state.stateMachine.cancelRequest(id: requestID)
        }
    }

    /// Mark a connection as going away. Connection implementors have to call this method if the connection
    /// has received a close intent from the server. For example: an HTTP/2 GOWAY frame.
    public func connectionWillClose(_ connection: Connection) {

    }

    public func connectionReceivedNewMaxStreamSetting(_ connection: Connection, newMaxStreamSetting maxStreams: UInt16) {
        self.modifyStateAndRunActions { state in
            state.stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: maxStreams)
        }
    }

    public func run() async {
        await withTaskCancellationHandler {
            if #available(macOS 14.0, iOS 17.0, tvOS 17.0, watchOS 10.0, *) {
                return await withDiscardingTaskGroup() { taskGroup in
                    await self.run(in: &taskGroup)
                }
            }
            return await withTaskGroup(of: Void.self) { taskGroup in
                await self.run(in: &taskGroup)
            }
        } onCancel: {
            let actions = self.stateBox.withLockedValue { state in
                state.stateMachine.triggerForceShutdown()
            }

            self.runStateMachineActions(actions)
        }
    }

    // MARK: - Private Methods -

    @inlinable
    func connectionDidClose(_ connection: Connection, error: (any Error)?) {
        self.observabilityDelegate.connectionClosed(id: connection.id, error: error)

        self.modifyStateAndRunActions { state in
            state.stateMachine.connectionClosed(connection)
        }
    }

    // MARK: Events

    @usableFromInline
    enum NewPoolActions: Sendable {
        case makeConnection(StateMachine.ConnectionRequest)
        case runKeepAlive(Connection)

        case scheduleTimer(StateMachine.Timer)
    }

    @available(macOS 14.0, iOS 17.0, tvOS 17.0, watchOS 10.0, *)
    private func run(in taskGroup: inout DiscardingTaskGroup) async {
        for await event in self.eventStream {
            self.runEvent(event, in: &taskGroup)
        }
    }

    private func run(in taskGroup: inout TaskGroup<Void>) async {
        var running = 0
        for await event in self.eventStream {
            running += 1
            self.runEvent(event, in: &taskGroup)

            if running == 100 {
                _ = await taskGroup.next()
                running -= 1
            }
        }
    }

    private func runEvent(_ event: NewPoolActions, in taskGroup: inout some TaskGroupProtocol) {
        switch event {
        case .makeConnection(let request):
            self.makeConnection(for: request, in: &taskGroup)

        case .runKeepAlive(let connection):
            self.runKeepAlive(connection, in: &taskGroup)

        case .scheduleTimer(let timer):
            self.runTimer(timer, in: &taskGroup)
        }
    }

    // MARK: Run actions

    @inlinable
    /*private*/ func modifyStateAndRunActions(_ closure: (inout State) -> StateMachine.Action) {
        let actions = self.stateBox.withLockedValue { state -> StateMachine.Action in
            closure(&state)
        }
        self.runStateMachineActions(actions)
    }

    @inlinable
    /*private*/ func runStateMachineActions(_ actions: StateMachine.Action) {
        self.runConnectionAction(actions.connection)
        self.runRequestAction(actions.request)
    }

    @inlinable
    /*private*/ func runConnectionAction(_ action: StateMachine.ConnectionAction) {
        switch action {
        case .makeConnection(let request, let timers):
            self.cancelTimers(timers)
            self.eventContinuation.yield(.makeConnection(request))

        case .runKeepAlive(let connection, let cancelContinuation):
            cancelContinuation?.resume(returning: ())
            self.eventContinuation.yield(.runKeepAlive(connection))

        case .scheduleTimers(let timers):
            for timer in timers {
                self.eventContinuation.yield(.scheduleTimer(timer))
            }

        case .cancelTimers(let timers):
            self.cancelTimers(timers)

        case .closeConnection(let connection, let timers):
            self.closeConnection(connection)
            self.cancelTimers(timers)

        case .shutdown(let cleanup):
            for connection in cleanup.connections {
                self.closeConnection(connection)
            }
            self.cancelTimers(cleanup.timersToCancel)

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func runRequestAction(_ action: StateMachine.RequestAction) {
        switch action {
        case .leaseConnection(let requests, let connection):
            for request in requests {
                request.complete(with: .success(connection))
            }

        case .failRequest(let request, let error):
            request.complete(with: .failure(error))

        case .failRequests(let requests, let error):
            for request in requests { request.complete(with: .failure(error)) }

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func makeConnection(for request: StateMachine.ConnectionRequest, in taskGroup: inout some TaskGroupProtocol) {
        taskGroup.addTask_ {
            self.observabilityDelegate.startedConnecting(id: request.connectionID)

            do {
                let bundle = try await self.factory(request.connectionID, self)
                self.connectionEstablished(bundle)

                // after the connection has been established, we keep the task open. This ensures
                // that the pools run method can not be exited before all connections have been
                // closed.
                await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                    bundle.connection.onClose {
                        self.connectionDidClose(bundle.connection, error: $0)
                        continuation.resume()
                    }
                }
            } catch {
                self.connectionEstablishFailed(error, for: request)
            }
        }
    }

    @inlinable
    /*private*/ func connectionEstablished(_ connectionBundle: ConnectionAndMetadata<Connection>) {
        self.observabilityDelegate.connectSucceeded(id: connectionBundle.connection.id, streamCapacity: connectionBundle.maximalStreamsOnConnection)

        self.modifyStateAndRunActions { state in
            state.lastConnectError = nil
            return state.stateMachine.connectionEstablished(
                connectionBundle.connection,
                maxStreams: connectionBundle.maximalStreamsOnConnection
            )
        }
    }

    @inlinable
    /*private*/ func connectionEstablishFailed(_ error: Error, for request: StateMachine.ConnectionRequest) {
        self.observabilityDelegate.connectFailed(id: request.connectionID, error: error)

        self.modifyStateAndRunActions { state in
            state.lastConnectError = error
            return state.stateMachine.connectionEstablishFailed(error, for: request)
        }
    }

    @inlinable
    /*private*/ func runKeepAlive(_ connection: Connection, in taskGroup: inout some TaskGroupProtocol) {
        self.observabilityDelegate.keepAliveTriggered(id: connection.id)

        taskGroup.addTask_ {
            do {
                try await self.keepAliveBehavior.runKeepAlive(for: connection)

                self.observabilityDelegate.keepAliveSucceeded(id: connection.id)

                self.modifyStateAndRunActions { state in
                    state.stateMachine.connectionKeepAliveDone(connection)
                }
            } catch {
                self.observabilityDelegate.keepAliveFailed(id: connection.id, error: error)

                self.modifyStateAndRunActions { state in
                    state.stateMachine.connectionKeepAliveFailed(connection.id)
                }
            }
        }
    }

    @inlinable
    /*private*/ func closeConnection(_ connection: Connection) {
        self.observabilityDelegate.connectionClosing(id: connection.id)

        connection.close()
    }

    @usableFromInline
    enum TimerRunResult: Sendable {
        case timerTriggered
        case timerCancelled
        case cancellationContinuationFinished
    }

    @inlinable
    /*private*/ func runTimer(_ timer: StateMachine.Timer, in poolGroup: inout some TaskGroupProtocol) {
        poolGroup.addTask_ { () async -> () in
            await withTaskGroup(of: TimerRunResult.self, returning: Void.self) { taskGroup in
                taskGroup.addTask {
                    do {
                        try await self.clock.sleep(for: timer.duration)
                        return .timerTriggered
                    } catch {
                        return .timerCancelled
                    }
                }

                taskGroup.addTask {
                    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                        let continuation = self.stateBox.withLockedValue { state in
                            state.stateMachine.timerScheduled(timer, cancelContinuation: continuation)
                        }

                        continuation?.resume(returning: ())
                    }

                    return .cancellationContinuationFinished
                }

                switch await taskGroup.next()! {
                case .cancellationContinuationFinished:
                    taskGroup.cancelAll()

                case .timerTriggered:
                    let action = self.stateBox.withLockedValue { state in
                        state.stateMachine.timerTriggered(timer)
                    }

                    self.runStateMachineActions(action)

                case .timerCancelled:
                    // the only way to reach this, is if the state machine decided to cancel the 
                    // timer. therefore we don't need to report it back!
                    break
                }

                return
            }
        }
    }

    @inlinable
    /*private*/ func cancelTimers(_ cancellationTokens: some Sequence<CheckedContinuation<Void, Never>>) {
        for token in cancellationTokens {
            token.resume()
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolConfiguration {
    init<KeepAliveBehavior: ConnectionKeepAliveBehavior>(_ configuration: ConnectionPoolConfiguration, keepAliveBehavior: KeepAliveBehavior) {
        self.minimumConnectionCount = configuration.minimumConnectionCount
        self.maximumConnectionSoftLimit = configuration.maximumConnectionSoftLimit
        self.maximumConnectionHardLimit = configuration.maximumConnectionHardLimit
        self.keepAliveDuration = keepAliveBehavior.keepAliveFrequency
        self.idleTimeoutDuration = configuration.idleTimeout
    }
}

@usableFromInline
protocol TaskGroupProtocol {
    // We need to call this `addTask_` because some Swift versions define this
    // under exactly this name and others have different attributes. So let's pick
    // a name that doesn't clash anywhere and implement it using the standard `addTask`.
    mutating func addTask_(operation: @escaping @Sendable () async -> Void)
}

@available(macOS 14.0, iOS 17.0, tvOS 17.0, watchOS 10.0, *)
extension DiscardingTaskGroup: TaskGroupProtocol {
    @inlinable
    mutating func addTask_(operation: @escaping @Sendable () async -> Void) {
        self.addTask(priority: nil, operation: operation)
    }
}

extension TaskGroup<Void>: TaskGroupProtocol {
    @inlinable
    mutating func addTask_(operation: @escaping @Sendable () async -> Void) {
        self.addTask(priority: nil, operation: operation)
    }
}
