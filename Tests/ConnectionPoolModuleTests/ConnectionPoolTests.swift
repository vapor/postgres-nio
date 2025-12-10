@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Atomics
import NIOEmbedded
import Testing


@Suite struct ConnectionPoolTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func test1000ConsecutiveRequestsOnSingleConnection() async {
        let factory = MockConnectionFactory<ContinuousClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        // the same connection is reused 1000 times

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
            }

            let createdConnection = await factory.nextConnectAttempt { _ in
                return 1
            }

            do {
                for _ in 0..<1000 {
                    async let connectionFuture = pool.leaseConnection()
                    var connectionLease: ConnectionLease<MockConnection>?
                    #expect(factory.pendingConnectionAttemptsCount == 0)
                    connectionLease = try await connectionFuture
                    #expect(connectionLease != nil)
                    #expect(createdConnection === connectionLease?.connection)

                    connectionLease?.release()
                }
            } catch {
                Issue.record("Unexpected error: \(error)")
            }

            taskGroup.cancelAll()

            #expect(factory.pendingConnectionAttemptsCount == 0)
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }

        #expect(factory.runningConnections.count == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testShutdownPoolWhileConnectionIsBeingCreated() async {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
            }

            let (blockCancelStream, blockCancelContinuation) = AsyncStream.makeStream(of: Void.self)
            let (blockConnCreationStream, blockConnCreationContinuation) = AsyncStream.makeStream(of: Void.self)

            taskGroup.addTask_ {
                _ = try? await factory.nextConnectAttempt { _ in
                    blockCancelContinuation.yield()
                    var iterator = blockConnCreationStream.makeAsyncIterator()
                    await iterator.next()
                    throw ConnectionCreationError()
                }
            }

            var iterator = blockCancelStream.makeAsyncIterator()
            await iterator.next()

            taskGroup.cancelAll()
            blockConnCreationContinuation.yield()
        }

        struct ConnectionCreationError: Error {}
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testShutdownPoolWhileConnectionIsBackingOff() async {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
            }

            _ = try? await factory.nextConnectAttempt { _ in
                throw ConnectionCreationError()
            }

            await clock.nextTimerScheduled()

            taskGroup.cancelAll()
        }

        struct ConnectionCreationError: Error {}
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionHardLimitIsRespected() async {
        let factory = MockConnectionFactory<ContinuousClock>()

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 8
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        let hasFinished = ManagedAtomic(false)
        let createdConnections = ManagedAtomic(0)
        let iterations = 10_000

        // the same connection is reused 1000 times

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
                #expect(hasFinished.compareExchange(expected: false, desired: true, ordering: .relaxed).original == false)
            }

            taskGroup.addTask_ {
                var usedConnectionIDs = Set<Int>()
                for _ in 0..<config.maximumConnectionHardLimit {
                    await factory.nextConnectAttempt { connectionID in
                        #expect(usedConnectionIDs.insert(connectionID).inserted == true)
                        createdConnections.wrappingIncrement(ordering: .relaxed)
                        return 1
                    }
                }


                #expect(factory.pendingConnectionAttemptsCount == 0)
            }

            let (stream, continuation) = AsyncStream.makeStream(of: Void.self)

            for _ in 0..<iterations {
                taskGroup.addTask_ {
                    do {
                        let connectionLease = try await pool.leaseConnection()
                        connectionLease.release()
                    } catch {
                        Issue.record("Unexpected error: \(error)")
                    }
                    continuation.yield()
                }
            }

            var leaseReleaseIterator = stream.makeAsyncIterator()
            for _ in 0..<iterations {
                _ = await leaseReleaseIterator.next()
            }

            taskGroup.cancelAll()

            #expect(hasFinished.load(ordering: .relaxed) == false)
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }

        #expect(createdConnections.load(ordering: .relaxed) == config.maximumConnectionHardLimit)
        #expect(hasFinished.load(ordering: .relaxed) == true)
        #expect(factory.runningConnections.count == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let connectionLeaseFuture = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let connectionLease = try await connectionLeaseFuture
            #expect(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            #expect(expectedInstants.remove(deadline1) != nil)
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            #expect(expectedInstants.remove(deadline2) != nil)
            #expect(expectedInstants.isEmpty == true)

            // move clock forward to keep alive
            let newTime = clock.now.advanced(by: keepAliveDuration)
            clock.advance(to: newTime)
            print("clock advanced to: \(newTime)")

            await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { print("keep alive 1 has run") }
                #expect(keepAliveConnection === connectionLease.connection)
                return true
            }

            // keep alive 2

            let deadline3 = await clock.nextTimerScheduled()
            #expect(deadline3 == clock.now.advanced(by: keepAliveDuration))
            print(deadline3)

            // race keep alive vs timeout
            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            taskGroup.cancelAll()

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveOnClose() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(20)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let connectionLeaseFuture = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let connectionLease = try await connectionLeaseFuture
            #expect(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            #expect(expectedInstants.remove(deadline1) != nil)
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            #expect(expectedInstants.remove(deadline2) != nil)
            #expect(expectedInstants.isEmpty)

            // move clock forward to keep alive
            let newTime = clock.now.advanced(by: keepAliveDuration)
            clock.advance(to: newTime)

            await keepAlive.nextKeepAlive { keepAliveConnection in
                #expect(keepAliveConnection === connectionLease.connection)
                return true
            }

            // keep alive 2
            let deadline3 = await clock.nextTimerScheduled()
            #expect(deadline3 == clock.now.advanced(by: keepAliveDuration))
            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            let failingKeepAliveDidRun = ManagedAtomic(false)
            // the following keep alive should not cause a crash
            _ = try? await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { 
                    #expect(failingKeepAliveDidRun
                        .compareExchange(expected: false, desired: true, ordering: .relaxed).original == false)
                }
                #expect(keepAliveConnection === connectionLease.connection)
                keepAliveConnection.close()
                throw CancellationError() // any error 
            } // will fail and it's expected
            #expect(failingKeepAliveDidRun.load(ordering: .relaxed) == true)

            taskGroup.cancelAll()

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveWorksRacesAgainstShutdown() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let connectionLeaseFuture = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let connectionLease = try await connectionLeaseFuture
            #expect(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            #expect(expectedInstants.remove(deadline1) != nil)
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            #expect(expectedInstants.remove(deadline2) != nil)
            #expect(expectedInstants.isEmpty)

            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { print("keep alive 1 has run") }
                #expect(keepAliveConnection === connectionLease.connection)
                return true
            }

            taskGroup.cancelAll()
            print("cancelled")

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testCancelConnectionRequestWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            let leaseTask = Task {
                _ = try await pool.leaseConnection()
            }

            let connectionAttemptWaiter = Future(of: Void.self)

            taskGroup.addTask {
                try await factory.nextConnectAttempt { connectionID in
                    connectionAttemptWaiter.yield(value: ())
                    throw CancellationError()
                }
            }

            try await connectionAttemptWaiter.success
            leaseTask.cancel()

            let taskResult = await leaseTask.result
            switch taskResult {
            case .success:
                Issue.record("Expected task failure")
            case .failure(let failure):
                #expect(failure as? ConnectionPoolError == .requestCancelled)
            }

            taskGroup.cancelAll()
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeasingMultipleConnectionsAtOnceWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 persisted connections
            for _ in 0..<4 {
                await factory.nextConnectAttempt { connectionID in
                    return 1
                }
            }

            // create 4 connection requests
            let requests = (0..<4).map { ConnectionFuture(id: $0) }

            // lease 4 connections at once
            pool.leaseConnections(requests)
            var connectionLeases = [ConnectionLease<MockConnection>]()

            for request in requests {
                let connection = try await request.future.success
                connectionLeases.append(connection)
            }

            // Ensure that we got 4 distinct connections
            #expect(Set(connectionLeases.lazy.map(\.connection.id)).count == 4)

            // release all 4 leased connections
            for lease in connectionLeases {
                lease.release()
            }

            // shutdown
            taskGroup.cancelAll()
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeasingConnectionAfterShutdownIsInvokedFails() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 persisted connections
            for _ in 0..<4 {
                await factory.nextConnectAttempt { connectionID in
                    return 1
                }
            }

            // shutdown
            taskGroup.cancelAll()

            do {
                _ = try await pool.leaseConnection()
                Issue.record("Expected a failure")
            } catch {
                print("failed")
                #expect(error as? ConnectionPoolError == .poolShutdown)
            }

            print("will close connections: \(factory.runningConnections)")
            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeasingConnectionsAfterShutdownIsInvokedFails() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 persisted connections
            for _ in 0..<4 {
                await factory.nextConnectAttempt { connectionID in
                    return 1
                }
            }

            // shutdown
            taskGroup.cancelAll()

            // create 4 connection requests
            let requests = (0..<4).map { ConnectionFuture(id: $0) }

            // lease 4 connections at once
            pool.leaseConnections(requests)

            for request in requests {
                do {
                    _ = try await request.future.success
                    Issue.record("Expected a failure")
                } catch {
                    #expect(error as? ConnectionPoolError == .poolShutdown)
                }
            }

            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeasingMultipleStreamsFromOneConnectionWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 10
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 connection requests
            let requests = (0..<10).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)
            var connectionLeases = [ConnectionLease<MockConnection>]()

            await factory.nextConnectAttempt { connectionID in
                return 10
            }

            for request in requests {
                let connection = try await request.future.success
                connectionLeases.append(connection)
            }

            // Ensure that all requests got the same connection
            #expect(Set(connectionLeases.lazy.map(\.connection.id)).count == 1)

            // release all 10 leased streams
            for lease in connectionLeases {
                lease.release()
            }

            for _ in 0..<9 {
                _ = try? await factory.nextConnectAttempt { connectionID in
                    throw CancellationError()
                }
            }

            // shutdown
            taskGroup.cancelAll()
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testIncreasingAvailableStreamsWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 connection requests
            var requests = (0..<21).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)
            var connectionLease = [ConnectionLease<MockConnection>]()

            await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let lease = try await requests.first!.future.success
            connectionLease.append(lease)
            requests.removeFirst()

            pool.connectionReceivedNewMaxStreamSetting(lease.connection, newMaxStreamSetting: 21)

            for (_, request) in requests.enumerated() {
                let connection = try await request.future.success
                connectionLease.append(connection)
            }

            // Ensure that all requests got the same connection
            #expect(Set(connectionLease.lazy.map(\.connection.id)).count == 1)

            requests = (22..<42).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)

            // release all 21 leased streams in a single call
            pool.releaseConnection(lease.connection, streams: 21)

            // ensure all 20 new requests got fulfilled
            for request in requests {
                let connection = try await request.future.success
                connectionLease.append(connection)
            }

            // release all 20 leased streams one by one
            for _ in requests {
                pool.releaseConnection(lease.connection, streams: 1)
            }

            // shutdown
            taskGroup.cancelAll()
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testForceShutdown() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 1
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }
            await factory.nextConnectAttempt { connectionID in
                return 1
            }
            let lease = try await pool.leaseConnection()
            pool.releaseConnection(lease.connection)

            pool.triggerForceShutdown()

            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testForceShutdownWithLeasedConnection() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 1
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }
            await factory.nextConnectAttempt { connectionID in
                return 1
            }
            let lease = try await pool.leaseConnection()

            pool.triggerForceShutdown()

            pool.releaseConnection(lease.connection)

            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testForceShutdownWithActiveRequest() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }
            let connectionAttemptWaiter = Future(of: Void.self)
            let triggerShutdownWaiter = Future(of: Void.self)
            let leaseFailedWaiter = Future(of: Void.self)

            taskGroup.addTask {
                await #expect(throws: ConnectionPoolError.poolShutdown) {
                    try await pool.leaseConnection()
                }
                leaseFailedWaiter.yield(value: ())
            }

            taskGroup.addTask {
                try await factory.nextConnectAttempt { connectionID in
                    connectionAttemptWaiter.yield(value: ())
                    try await triggerShutdownWaiter.success
                    return 1
                }
            }
            try await connectionAttemptWaiter.success

            pool.triggerForceShutdown()

            triggerShutdownWaiter.yield(value: ())

            try await leaseFailedWaiter.success

            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }
}

struct ConnectionFuture: ConnectionRequestProtocol {
    let id: Int
    let future: Future<ConnectionLease<MockConnection>>

    init(id: Int) {
        self.id = id
        self.future = Future(of: ConnectionLease<MockConnection>.self)
    }

    func complete(with result: Result<ConnectionLease<MockConnection>, ConnectionPoolError>) {
        switch result {
        case .success(let success):
            self.future.yield(value: success)
        case .failure(let failure):
            self.future.yield(error: failure)
        }
    }
}
