@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PoolStateMachine_ConnectionGroupTests: XCTestCase {
    var idGenerator: ConnectionIDGenerator!

    let executor = NothingConnectionPoolExecutor()

    override func setUp() {
        self.idGenerator = ConnectionIDGenerator()
        super.setUp()
    }

    override func tearDown() {
        self.idGenerator = nil
        super.tearDown()
    }

    func testRefillConnections() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 4,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        XCTAssertTrue(connections.isEmpty)
        let requests = connections.refillConnections()
        XCTAssertFalse(connections.isEmpty)

        XCTAssertEqual(requests.count, 4)
        XCTAssertNil(connections.createNewDemandConnectionIfPossible())
        XCTAssertNil(connections.createNewOverflowConnectionIfPossible())
        XCTAssertEqual(connections.stats, .init(connecting: 4))
        XCTAssertEqual(connections.soonAvailableConnections, 4)

        let requests2 = connections.refillConnections()
        XCTAssertTrue(requests2.isEmpty)

        var connected: UInt16 = 0
        for request in requests {
            let newConnection = MockConnection(id: request.connectionID, executor: self.executor)
            let (_, context) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
            XCTAssertEqual(context.info, .idle(availableStreams: 1, newIdle: true))
            XCTAssertEqual(context.use, .persisted)
            connected += 1
            XCTAssertEqual(connections.stats, .init(connecting: 4 - connected, idle: connected, availableStreams: connected))
            XCTAssertEqual(connections.soonAvailableConnections, 4 - connected)
        }

        let requests3 = connections.refillConnections()
        XCTAssertTrue(requests3.isEmpty)
    }

    func testMakeConnectionLeaseItAndDropItHappyPath() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        XCTAssertTrue(connections.isEmpty)
        XCTAssertTrue(requests.isEmpty)

        guard let request = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to receive a connection request")
        }
        XCTAssertEqual(request, .init(connectionID: 0))
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(connections.soonAvailableConnections, 1)
        XCTAssertEqual(connections.stats, .init(connecting: 1))

        let newConnection = MockConnection(id: request.connectionID, executor: self.executor)
        let (_, establishedContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        XCTAssertEqual(establishedContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailableConnections, 0)

        guard case .leasedConnection(let leaseResult) = connections.leaseConnectionOrSoonAvailableConnectionCount() else {
            return XCTFail("Expected to lease a connection")
        }
        XCTAssert(newConnection === leaseResult.connection)
        XCTAssertEqual(connections.stats, .init(leased: 1, leasedStreams: 1))

        guard let (index, releasedContext) = connections.releaseConnection(leaseResult.connection.id, streams: 1) else {
            return XCTFail("Expected that this connection is still active")
        }
        XCTAssertEqual(releasedContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        let parkTimers = connections.parkConnection(at: index, hasBecomeIdle: true)
        XCTAssertEqual(parkTimers, [
            .init(timerID: 0, connectionID: newConnection.id, usecase: .keepAlive),
            .init(timerID: 1, connectionID: newConnection.id, usecase: .idleTimeout),
        ])

        guard let keepAliveAction = connections.keepAliveIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssert(newConnection === keepAliveAction.connection)
        XCTAssertEqual(connections.stats, .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, pingPongContext) = connections.keepAliveSucceeded(newConnection.id) else {
            return XCTFail("Expected to get an AvailableContext")
        }
        XCTAssertEqual(pingPongContext.info, .idle(availableStreams: 1, newIdle: false))
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        guard let closeAction = connections.closeConnectionIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssertEqual(closeAction.timersToCancel, [])
        XCTAssert(closeAction.connection === newConnection)
        XCTAssertEqual(connections.stats, .init(closing: 1, availableStreams: 0))

        let closeContext = connections.connectionClosed(newConnection.id)
        XCTAssertEqual(closeContext.connectionsStarting, 0)
        XCTAssertTrue(connections.isEmpty)
        XCTAssertEqual(connections.stats, .init())
    }

    func testBackoffDoneCreatesANewConnectionToReachMinimumConnectionsEvenThoughRetryIsSetToFalse() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        XCTAssertEqual(connections.stats, .init(connecting: 1))
        XCTAssertEqual(connections.soonAvailableConnections, 1)
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(requests.count, 1)

        guard let request = requests.first else { return XCTFail("Expected to receive a connection request") }
        XCTAssertEqual(request, .init(connectionID: 0))

        let backoffTimer = connections.backoffNextConnectionAttempt(request.connectionID)
        XCTAssertEqual(connections.stats, .init(backingOff: 1))
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        XCTAssertNil(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken))

        let backoffDoneAction = connections.backoffDone(request.connectionID, retry: false)
        XCTAssertEqual(backoffDoneAction, .createConnection(.init(connectionID: 0), backoffTimerCancellationToken))

        XCTAssertEqual(connections.stats, .init(connecting: 1))
    }

    func testBackoffDoneCancelsIdleTimerIfAPersistedConnectionIsNotRetried() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 2,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        XCTAssertEqual(connections.stats, .init(connecting: 2))
        XCTAssertEqual(connections.soonAvailableConnections, 2)
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(requests.count, 2)

        var requestIterator = requests.makeIterator()
        guard let firstRequest = requestIterator.next(), let secondRequest = requestIterator.next() else {
            return XCTFail("Expected to get two requests")
        }

        guard let thirdRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get another request")
        }
        XCTAssertEqual(connections.stats, .init(connecting: 3))

        let newSecondConnection = MockConnection(id: secondRequest.connectionID, executor: self.executor)
        let (_, establishedSecondConnectionContext) = connections.newConnectionEstablished(newSecondConnection, maxStreams: 1)
        XCTAssertEqual(establishedSecondConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedSecondConnectionContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(connecting: 2, idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailableConnections, 2)

        let newThirdConnection = MockConnection(id: thirdRequest.connectionID, executor: self.executor)
        let (thirdConnectionIndex, establishedThirdConnectionContext) = connections.newConnectionEstablished(newThirdConnection, maxStreams: 1)
        XCTAssertEqual(establishedThirdConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedThirdConnectionContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(connecting: 1, idle: 2, availableStreams: 2))
        XCTAssertEqual(connections.soonAvailableConnections, 1)
        let thirdConnKeepTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: thirdRequest.connectionID, usecase: .keepAlive)
        let thirdConnIdleTimer = TestPoolStateMachine.ConnectionTimer(timerID: 1, connectionID: thirdRequest.connectionID, usecase: .idleTimeout)
        let thirdConnIdleTimerCancellationToken = MockTimerCancellationToken(thirdConnIdleTimer)
        XCTAssertEqual(connections.parkConnection(at: thirdConnectionIndex, hasBecomeIdle: true), [thirdConnKeepTimer, thirdConnIdleTimer])

        XCTAssertNil(connections.timerScheduled(thirdConnKeepTimer, cancelContinuation: .init(thirdConnKeepTimer)))
        XCTAssertNil(connections.timerScheduled(thirdConnIdleTimer, cancelContinuation: thirdConnIdleTimerCancellationToken))

        let backoffTimer = connections.backoffNextConnectionAttempt(firstRequest.connectionID)
        XCTAssertEqual(connections.stats, .init(backingOff: 1, idle: 2, availableStreams: 2))
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        XCTAssertNil(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken))
        XCTAssertEqual(connections.stats, .init(backingOff: 1, idle: 2, availableStreams: 2))

        // connection three should be moved to connection one and for this reason become permanent

        XCTAssertEqual(connections.backoffDone(firstRequest.connectionID, retry: false), .cancelTimers([backoffTimerCancellationToken, thirdConnIdleTimerCancellationToken]))
        XCTAssertEqual(connections.stats, .init(idle: 2, availableStreams: 2))

        XCTAssertNil(connections.closeConnectionIfIdle(newThirdConnection.id))
    }

    func testBackoffDoneReturnsNilIfOverflowConnection() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        guard let firstRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get two requests")
        }

        guard let secondRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get another request")
        }
        XCTAssertEqual(connections.stats, .init(connecting: 2))

        let newFirstConnection = MockConnection(id: firstRequest.connectionID, executor: self.executor)
        let (_, establishedFirstConnectionContext) = connections.newConnectionEstablished(newFirstConnection, maxStreams: 1)
        XCTAssertEqual(establishedFirstConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedFirstConnectionContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(connecting: 1, idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailableConnections, 1)

        let backoffTimer = connections.backoffNextConnectionAttempt(secondRequest.connectionID)
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        XCTAssertEqual(connections.stats, .init(backingOff: 1, idle: 1, availableStreams: 1))
        XCTAssertNil(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken))

        XCTAssertEqual(connections.backoffDone(secondRequest.connectionID, retry: false), .cancelTimers([backoffTimerCancellationToken]))
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        XCTAssertNotNil(connections.closeConnectionIfIdle(newFirstConnection.id))
    }

    func testPingPong() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(connections.stats, .init(connecting: 1))

        XCTAssertEqual(requests.count, 1)
        guard let firstRequest = requests.first else { return XCTFail("Expected to have a request here") }

        let newConnection = MockConnection(id: firstRequest.connectionID, executor: self.executor)
        let (connectionIndex, establishedConnectionContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        XCTAssertEqual(establishedConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedConnectionContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
        let timers = connections.parkConnection(at: connectionIndex, hasBecomeIdle: true)
        let keepAliveTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: firstRequest.connectionID, usecase: .keepAlive)
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        XCTAssertEqual(timers, [keepAliveTimer])
        XCTAssertNil(connections.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))
        let keepAliveAction = connections.keepAliveIfIdle(newConnection.id)
        XCTAssertEqual(keepAliveAction, .init(connection: newConnection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken))
        XCTAssertEqual(connections.stats, .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, afterPingIdleContext) = connections.keepAliveSucceeded(newConnection.id) else {
            return XCTFail("Expected to receive an AvailableContext")
        }
        XCTAssertEqual(afterPingIdleContext.info, .idle(availableStreams: 1, newIdle: false))
        XCTAssertEqual(afterPingIdleContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
    }

    func testKeepAliveShouldNotIndicateCloseConnectionAfterClosed() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 2,
            maximumConcurrentConnectionHardLimit: 2,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        guard let firstRequest = connections.createNewDemandConnectionIfPossible() else { return XCTFail("Expected to have a request here") }

        let newConnection = MockConnection(id: firstRequest.connectionID, executor: self.executor)
        let (connectionIndex, establishedConnectionContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        XCTAssertEqual(establishedConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
        _ = connections.parkConnection(at: connectionIndex, hasBecomeIdle: true)
        let keepAliveTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: firstRequest.connectionID, usecase: .keepAlive)
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        XCTAssertNil(connections.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))
        let keepAliveAction = connections.keepAliveIfIdle(newConnection.id)
        XCTAssertEqual(keepAliveAction, .init(connection: newConnection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken))
        XCTAssertEqual(connections.stats, .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        _ = connections.closeConnectionIfIdle(newConnection.id)
        guard connections.keepAliveFailed(newConnection.id) == nil else {
            return XCTFail("Expected keepAliveFailed not to cause close again")
        }
        XCTAssertEqual(connections.stats, .init(closing: 1))
    }
}
