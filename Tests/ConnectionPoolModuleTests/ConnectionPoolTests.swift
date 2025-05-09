@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Atomics
import NIOEmbedded
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ConnectionPoolTests: XCTestCase {

    func test1000ConsecutiveRequestsOnSingleConnection() async {
        let factory = MockConnectionFactory<ContinuousClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
                    var leasedConnection: MockConnection?
                    XCTAssertEqual(factory.pendingConnectionAttemptsCount, 0)
                    leasedConnection = try await connectionFuture
                    XCTAssertNotNil(leasedConnection)
                    XCTAssert(createdConnection === leasedConnection)

                    if let leasedConnection {
                        pool.releaseConnection(leasedConnection)
                    }
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
        let factory = MockConnectionFactory<MockClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
        let factory = MockConnectionFactory<MockClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
        let factory = MockConnectionFactory<ContinuousClock>()

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
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
                        let leasedConnection = try await pool.leaseConnection()
                        pool.releaseConnection(leasedConnection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let lease1ConnectionAsync = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let lease1Connection = try await lease1ConnectionAsync
            XCTAssert(connection === lease1Connection)

            pool.releaseConnection(lease1Connection)

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
                XCTAssertTrue(keepAliveConnection === lease1Connection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let lease1ConnectionAsync = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let lease1Connection = try await lease1ConnectionAsync
            XCTAssert(connection === lease1Connection)

            pool.releaseConnection(lease1Connection)

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
                XCTAssertTrue(keepAliveConnection === lease1Connection)
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
                XCTAssertTrue(keepAliveConnection === lease1Connection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, configuration: $1, for: $2)
        }

        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            async let lease1ConnectionAsync = pool.leaseConnection()

            let connection = await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let lease1Connection = try await lease1ConnectionAsync
            XCTAssert(connection === lease1Connection)

            pool.releaseConnection(lease1Connection)

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
                XCTAssertTrue(keepAliveConnection === lease1Connection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            var connections = [MockConnection]()

            for request in requests {
                let connection = try await request.future.success
                connections.append(connection)
            }

            // Ensure that we got 4 distinct connections
            XCTAssertEqual(Set(connections.lazy.map(\.id)).count, 4)

            // release all 4 leased connections
            for connection in connections {
                pool.releaseConnection(connection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            var connections = [MockConnection]()

            await factory.nextConnectAttempt { connectionID in
                return 10
            }

            for request in requests {
                let connection = try await request.future.success
                connections.append(connection)
            }

            // Ensure that all requests got the same connection
            XCTAssertEqual(Set(connections.lazy.map(\.id)).count, 1)

            // release all 10 leased streams
            for connection in connections {
                pool.releaseConnection(connection)
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
            connectionConfiguration: MockConnectionConfiguration(username: "username", password: "password"),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionFuture.self,
            keepAliveBehavior: keepAlive,
            executor: MockExecutor(),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
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
            var connections = [MockConnection]()

            await factory.nextConnectAttempt { connectionID in
                return 1
            }

            let connection = try await requests.first!.future.success
            connections.append(connection)
            requests.removeFirst()

            pool.connectionReceivedNewMaxStreamSetting(connection, newMaxStreamSetting: 21)

            for (_, request) in requests.enumerated() {
                let connection = try await request.future.success
                connections.append(connection)
            }

            // Ensure that all requests got the same connection
            XCTAssertEqual(Set(connections.lazy.map(\.id)).count, 1)

            requests = (22..<42).map { ConnectionFuture(id: $0) }
            pool.leaseConnections(requests)

            // release all 21 leased streams in a single call
            pool.releaseConnection(connection, streams: 21)

            // ensure all 20 new requests got fulfilled
            for request in requests {
                let connection = try await request.future.success
                connections.append(connection)
            }

            // release all 20 leased streams one by one
            for _ in requests {
                pool.releaseConnection(connection, streams: 1)
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
    let future: Future<MockConnection>

    init(id: Int) {
        self.id = id
        self.future = Future(of: MockConnection.self)
    }

    func complete(with result: Result<MockConnection, ConnectionPoolError>) {
        switch result {
        case .success(let success):
            self.future.yield(value: success)
        case .failure(let failure):
            self.future.yield(error: failure)
        }
    }
}
