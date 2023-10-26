@testable import _ConnectionPoolModule
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
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
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
        }
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
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
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
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
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

            await clock.timerScheduled()

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
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        // the same connection is reused 1000 times

        await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            taskGroup.addTask {
                var usedConnectionIDs = Set<Int>()
                for _ in 0..<config.maximumConnectionHardLimit {
                    await factory.nextConnectAttempt { connectionID in
                        XCTAssertTrue(usedConnectionIDs.insert(connectionID).inserted)
                        return 1
                    }
                }

                XCTAssertEqual(factory.pendingConnectionAttemptsCount, 0)
            }

            for _ in 0..<10_000 {
                taskGroup.addTask {
                    let leasedConnection = try await pool.leaseConnection()
                    pool.releaseConnection(leasedConnection)
                }
            }

            taskGroup.cancelAll()
        }
    }
}


