@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Atomics
import NIOEmbedded
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ConnectionPoolTests: XCTestCase {

    let executor = MockExecutor()

    func test1000ConsecutiveRequestsOnSingleConnection() async {
        let factory = MockConnectionFactory<ContinuousClock, MockExecutor>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: self.executor,
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        // the same connection is reused 1000 times

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
            }

            let createdConnection = await factory.nextConnectAttempt { _ in
                return 1
            }
            XCTAssertNotNil(createdConnection)

            do {
                for _ in 0..<1000 {
                    async let connectionFuture = try await pool.leaseConnection()
                    var connectionLease: ConnectionLease<MockConnection<MockExecutor>>?
                    XCTAssertEqual(factory.pendingConnectionAttemptsCount, 0)
                    connectionLease = try await connectionFuture
                    XCTAssertNotNil(connectionLease)
                    XCTAssert(createdConnection === connectionLease?.connection)

                    connectionLease?.release()
                }
            } catch {
                XCTFail("Unexpected error: \(error)")
            }

            taskGroup.cancelAll()

            XCTAssertEqual(factory.pendingConnectionAttemptsCount, 0)
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }

        XCTAssertEqual(factory.runningConnections.count, 0)
    }

    func testShutdownPoolWhileConnectionIsBeingCreated() async {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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

    func testShutdownPoolWhileConnectionIsBackingOff() async {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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

    func testConnectionHardLimitIsRespected() async {
        let factory = MockConnectionFactory<ContinuousClock, MockExecutor>()

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 8
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        let hasFinished = ManagedAtomic(false)
        let createdConnections = ManagedAtomic(0)
        let iterations = 10_000

        // the same connection is reused 1000 times

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask_ {
                await pool.run()
                XCTAssertFalse(hasFinished.compareExchange(expected: false, desired: true, ordering: .relaxed).original)
            }

            taskGroup.addTask_ {
                var usedConnectionIDs = Set<Int>()
                for _ in 0..<config.maximumConnectionHardLimit {
                    await factory.nextConnectAttempt { connectionID in
                        XCTAssertTrue(usedConnectionIDs.insert(connectionID).inserted)
                        createdConnections.wrappingIncrement(ordering: .relaxed)
                        return 1
                    }
                }


                XCTAssertEqual(factory.pendingConnectionAttemptsCount, 0)
            }

            let (stream, continuation) = AsyncStream.makeStream(of: Void.self)

            for _ in 0..<iterations {
                taskGroup.addTask_ {
                    do {
                        let connectionLease = try await pool.leaseConnection()
                        connectionLease.release()
                    } catch {
                        XCTFail("Unexpected error: \(error)")
                    }
                    continuation.yield()
                }
            }

            var leaseReleaseIterator = stream.makeAsyncIterator()
            for _ in 0..<iterations {
                _ = await leaseReleaseIterator.next()
            }

            taskGroup.cancelAll()

            XCTAssertFalse(hasFinished.load(ordering: .relaxed))
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }

        XCTAssertEqual(createdConnections.load(ordering: .relaxed), config.maximumConnectionHardLimit)
        XCTAssert(hasFinished.load(ordering: .relaxed))
        XCTAssertEqual(factory.runningConnections.count, 0)
    }

    func testKeepAliveWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
            XCTAssert(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            XCTAssertNotNil(expectedInstants.remove(deadline1))
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            XCTAssertNotNil(expectedInstants.remove(deadline2))
            XCTAssert(expectedInstants.isEmpty)

            // move clock forward to keep alive
            let newTime = clock.now.advanced(by: keepAliveDuration)
            clock.advance(to: newTime)
            print("clock advanced to: \(newTime)")

            await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { print("keep alive 1 has run") }
                XCTAssertTrue(keepAliveConnection === connectionLease.connection)
                return true
            }

            // keep alive 2

            let deadline3 = await clock.nextTimerScheduled()
            XCTAssertEqual(deadline3, clock.now.advanced(by: keepAliveDuration))
            print(deadline3)

            // race keep alive vs timeout
            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            taskGroup.cancelAll()

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    func testKeepAliveOnClose() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(20)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
            XCTAssert(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            XCTAssertNotNil(expectedInstants.remove(deadline1))
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            XCTAssertNotNil(expectedInstants.remove(deadline2))
            XCTAssert(expectedInstants.isEmpty)

            // move clock forward to keep alive
            let newTime = clock.now.advanced(by: keepAliveDuration)
            clock.advance(to: newTime)

            await keepAlive.nextKeepAlive { keepAliveConnection in
                XCTAssertTrue(keepAliveConnection === connectionLease.connection)
                return true
            }

            // keep alive 2
            let deadline3 = await clock.nextTimerScheduled()
            XCTAssertEqual(deadline3, clock.now.advanced(by: keepAliveDuration))
            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            let failingKeepAliveDidRun = ManagedAtomic(false)
            // the following keep alive should not cause a crash
            _ = try? await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { 
                    XCTAssertFalse(failingKeepAliveDidRun
                        .compareExchange(expected: false, desired: true, ordering: .relaxed).original)
                }
                XCTAssertTrue(keepAliveConnection === connectionLease.connection)
                keepAliveConnection.close()
                throw CancellationError() // any error 
            } // will fail and it's expected
            XCTAssertTrue(failingKeepAliveDidRun.load(ordering: .relaxed))

            taskGroup.cancelAll()

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    func testKeepAliveWorksRacesAgainstShutdown() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
            XCTAssert(connection === connectionLease.connection)

            connectionLease.release()

            // keep alive 1

            // validate that a keep alive timer and an idle timeout timer is scheduled
            var expectedInstants: Set<MockClock.Instant> = [.init(keepAliveDuration), .init(config.idleTimeout)]
            let deadline1 = await clock.nextTimerScheduled()
            print(deadline1)
            XCTAssertNotNil(expectedInstants.remove(deadline1))
            let deadline2 = await clock.nextTimerScheduled()
            print(deadline2)
            XCTAssertNotNil(expectedInstants.remove(deadline2))
            XCTAssert(expectedInstants.isEmpty)

            clock.advance(to: clock.now.advanced(by: keepAliveDuration))

            await keepAlive.nextKeepAlive { keepAliveConnection in
                defer { print("keep alive 1 has run") }
                XCTAssertTrue(keepAliveConnection === connectionLease.connection)
                return true
            }

            taskGroup.cancelAll()
            print("cancelled")

            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    func testCancelConnectionRequestWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
                XCTFail("Expected task failure")
            case .failure(let failure):
                XCTAssertEqual(failure as? ConnectionPoolError, .requestCancelled)
            }

            taskGroup.cancelAll()
            for connection in factory.runningConnections {
                connection.closeIfClosing()
            }
        }
    }

    func testLeasingMultipleConnectionsAtOnceWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
            var connectionLeases = [ConnectionLease<MockConnection<MockExecutor>>]()

            for request in requests {
                let connection = try await request.future.success
                connectionLeases.append(connection)
            }

            // Ensure that we got 4 distinct connections
            XCTAssertEqual(Set(connectionLeases.lazy.map(\.connection.id)).count, 4)

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

    func testLeasingConnectionAfterShutdownIsInvokedFails() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
                XCTFail("Expected a failure")
            } catch {
                print("failed")
                XCTAssertEqual(error as? ConnectionPoolError, .poolShutdown)
            }

            print("will close connections: \(factory.runningConnections)")
            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    func testLeasingConnectionsAfterShutdownIsInvokedFails() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 4
        mutableConfig.maximumConnectionSoftLimit = 4
        mutableConfig.maximumConnectionHardLimit = 4
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
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
                    XCTFail("Expected a failure")
                } catch {
                    XCTAssertEqual(error as? ConnectionPoolError, .poolShutdown)
                }
            }

            for connection in factory.runningConnections {
                try await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    func testLeasingMultipleStreamsFromOneConnectionWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 10
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 connection requests
            let requests = (0..<10).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)
            var connectionLeases = [ConnectionLease<MockConnection<MockExecutor>>]()

            await factory.nextConnectAttempt { connectionID in
                return 10
            }

            for request in requests {
                let connection = try await request.future.success
                connectionLeases.append(connection)
            }

            // Ensure that all requests got the same connection
            XCTAssertEqual(Set(connectionLeases.lazy.map(\.connection.id)).count, 1)

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

    func testIncreasingAvailableStreamsWorks() async throws {
        let clock = MockClock()
        let factory = MockConnectionFactory<MockClock, MockExecutor>()
        let keepAliveDuration = Duration.seconds(30)
        let keepAlive = MockPingPongBehavior(keepAliveFrequency: keepAliveDuration, connectionType: MockConnection<MockExecutor>.self)

        var mutableConfig = ConnectionPoolConfiguration()
        mutableConfig.minimumConnectionCount = 0
        mutableConfig.maximumConnectionSoftLimit = 1
        mutableConfig.maximumConnectionHardLimit = 1
        mutableConfig.idleTimeout = .seconds(10)
        let config = mutableConfig

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection<MockExecutor>.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            // create 4 connection requests
            var requests = (0..<21).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)
            var connectionLease = [ConnectionLease<MockConnection<MockExecutor>>]()

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
            XCTAssertEqual(Set(connectionLease.lazy.map(\.connection.id)).count, 1)

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
}

struct ConnectionFuture: ConnectionRequestProtocol {
    let id: Int
    let future: Future<ConnectionLease<MockConnection<MockExecutor>>>

    init(id: Int) {
        self.id = id
        self.future = Future(of: ConnectionLease<MockConnection>.self)
    }

    func complete(with result: Result<ConnectionLease<MockConnection<MockExecutor>>, ConnectionPoolError>) {
        switch result {
        case .success(let success):
            self.future.yield(value: success)
        case .failure(let failure):
            self.future.yield(error: failure)
        }
    }
}
