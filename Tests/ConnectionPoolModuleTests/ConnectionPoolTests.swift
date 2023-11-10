@testable import _ConnectionPoolModule
import Atomics
import XCTest
import NIOEmbedded

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ConnectionPoolTests: XCTestCase {

    func test1000ConsecutiveRequestsOnSingleConnection() async {
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
            taskGroup.addTask {
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
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            let (blockCancelStream, blockCancelContinuation) = AsyncStream.makeStream(of: Void.self)
            let (blockConnCreationStream, blockConnCreationContinuation) = AsyncStream.makeStream(of: Void.self)

            taskGroup.addTask {
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
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: clock
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
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
            taskGroup.addTask {
                await pool.run()
                XCTAssertFalse(hasFinished.compareExchange(expected: false, desired: true, ordering: .relaxed).original)
            }

            taskGroup.addTask {
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
                taskGroup.addTask {
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

}


