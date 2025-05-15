import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ConnectionPoolManagerConfiguration: Sendable {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes
    /// idle connections,
    /// the `ConnectionPool` will initiate new outbound
    /// connections proactively to avoid the number of available
    /// connections dropping below this number.
    public var minimumConnectionPerExecutorCount: Int

    /// Between the `minimumConnectionCount` and
    /// `maximumConnectionSoftLimit` the connection pool creates
    /// _preserved_ connections. Preserved connections are closed
    /// if they have been idle for ``idleTimeout``.
    public var maximumConnectionPerExecutorSoftLimit: Int

    /// The maximum number of connections for this pool, that can
    /// exist at any point in time. The pool can create _overflow_
    /// connections, if all connections are leased, and the
    /// `maximumConnectionHardLimit` > `maximumConnectionSoftLimit `
    /// Overflow connections are closed immediately as soon as they
    /// become idle.
    public var maximumConnectionPerExecutorHardLimit: Int

    /// The time that a _preserved_ idle connection stays in the
    /// pool before it is closed.
    public var idleTimeout: Duration

    /// initializer
    public init() {
        self.minimumConnectionPerExecutorCount = 0
        self.maximumConnectionPerExecutorSoftLimit = 16
        self.maximumConnectionPerExecutorHardLimit = 16
        self.idleTimeout = .seconds(60)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class ConnectionPoolManager<
    Connection: PooledConnection,
    ConnectionID: Hashable & Sendable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    ConnectionConfiguration: Equatable & Sendable,
    Request: ConnectionRequestProtocol,
    RequestID: Hashable & Sendable,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    Executor: ConnectionPoolExecutor,
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
    public typealias ConnectionFactory = @Sendable (ConnectionID, ConnectionConfiguration, ConnectionPool) async throws -> ConnectionAndMetadata<Connection>

    public typealias ConnectionPool = _ConnectionPoolModule.ConnectionPool<
        Connection,
        ConnectionID,
        ConnectionIDGenerator,
        ConnectionConfiguration,
        Request,
        RequestID,
        KeepAliveBehavior,
        Executor,
        ObservabilityDelegate,
        Clock
    >

    @usableFromInline
    let pools: [Executor.ID: ConnectionPool]

    @usableFromInline
    let roundRobinCounter = ManagedAtomic(0)

    @usableFromInline
    let roundRobinPools: [ConnectionPool]

    @usableFromInline
    let actionsStream: AsyncStream<Actions>

    @usableFromInline
    let eventContinuation: AsyncStream<Actions>.Continuation

    @inlinable
    public init(
        configuration: ConnectionPoolManagerConfiguration,
        connectionConfiguration: ConnectionConfiguration,
        idGenerator: ConnectionIDGenerator,
        requestType: Request.Type,
        keepAliveBehavior: KeepAliveBehavior,
        executors: [Executor],
        observabilityDelegate: ObservabilityDelegate,
        clock: Clock,
        connectionFactory: @escaping ConnectionFactory
    ) {
        let (stream, continuation) = AsyncStream.makeStream(of: Actions.self)
        self.actionsStream = stream
        self.eventContinuation = continuation

        var pools = [Executor.ID: ConnectionPool]()
        pools.reserveCapacity(executors.count)

        var singlePoolConfig = ConnectionPoolConfiguration()
        singlePoolConfig.minimumConnectionCount = configuration.minimumConnectionPerExecutorCount
        singlePoolConfig.maximumConnectionSoftLimit = configuration.maximumConnectionPerExecutorSoftLimit
        singlePoolConfig.maximumConnectionHardLimit = configuration.maximumConnectionPerExecutorHardLimit

        for executor in executors {
            pools[executor.id] = ConnectionPool(
                configuration: singlePoolConfig,
                connectionConfiguration: connectionConfiguration,
                idGenerator: idGenerator,
                requestType: requestType,
                keepAliveBehavior: keepAliveBehavior,
                executor: executor,
                observabilityDelegate: observabilityDelegate,
                clock: clock,
                connectionFactory: connectionFactory
            )
        }

        self.pools = pools
        self.roundRobinPools = Array(pools.values)

        for pool in pools.values {
            self.eventContinuation.yield(.runPool(pool))
        }
    }

    @inlinable
    public func leaseConnection(_ request: Request) {
        if let executorID = Executor.getExecutorID(), let pool = self.pools[executorID] {
            pool.leaseConnection(request)
        }

        let index = self.roundRobinCounter.loadThenWrappingIncrement(ordering: .relaxed) % self.roundRobinPools.count

        self.roundRobinPools[index].leaseConnection(request)
    }

    @inlinable
    public func cancelLeaseConnection(_ requestID: RequestID) {
        // TODO: This is expensive!
        for pool in self.roundRobinPools {
            if pool.cancelLeaseConnection(requestID) {
                break
            }
        }
    }

    @usableFromInline
    enum Actions: Sendable {
        case runPool(ConnectionPool)
    }

    @inlinable
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

        }
    }

    @inlinable
    public func updateConfiguration(_ configuration: ConnectionConfiguration, forceReconnection: Bool) {
        for pool in self.pools.values {
            pool.updateConfiguration(configuration, forceReconnection: forceReconnection)
        }
    }

    // MARK: - Private Methods -

    @available(macOS 14.0, iOS 17.0, tvOS 17.0, watchOS 10.0, *)
    @inlinable
    /* private */ func run(in taskGroup: inout DiscardingTaskGroup) async {
        for await event in self.actionsStream {
            self.runEvent(event, in: &taskGroup)
        }
    }

    @inlinable
    /* private */ func run(in taskGroup: inout TaskGroup<Void>) async {
        var running = 0
        for await event in self.actionsStream {
            running += 1
            self.runEvent(event, in: &taskGroup)

            if running == 100 {
                _ = await taskGroup.next()
                running -= 1
            }
        }
    }

    @inlinable
    /* private */ func runEvent(_ event: Actions, in taskGroup: inout some TaskGroupProtocol) {
        switch event {
        case .runPool(let pool):
            if #available(macOS 15.0, iOS 18.0, tvOS 18.0, watchOS 11.0, *), let executor = pool.executor as? TaskExecutor {
                taskGroup.addTask_(executorPreference: executor) {
                    await pool.run()
                }
            } else {
                taskGroup.addTask_ {
                    await pool.run()
                }
            }
        }
    }
}
