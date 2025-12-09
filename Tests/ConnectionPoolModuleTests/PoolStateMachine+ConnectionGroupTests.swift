@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct PoolStateMachine_ConnectionGroupTests {
    var idGenerator = ConnectionIDGenerator()

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRefillConnections() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 4,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        #expect(connections.isEmpty == true)
        let requests = connections.refillConnections()
        #expect(connections.isEmpty == false)

        #expect(requests.count == 4)
        #expect(connections.createNewDemandConnectionIfPossible() == nil)
        #expect(connections.createNewOverflowConnectionIfPossible() == nil)
        #expect(connections.stats == .init(connecting: 4))
        #expect(connections.soonAvailableConnections == 4)

        let requests2 = connections.refillConnections()
        #expect(requests2.isEmpty == true)

        var connected: UInt16 = 0
        for request in requests {
            let newConnection = MockConnection(id: request.connectionID)
            let (_, context) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
            #expect(context.info == .idle(availableStreams: 1, newIdle: true))
            #expect(context.use == .persisted)
            connected += 1
            #expect(connections.stats == .init(connecting: 4 - connected, idle: connected, availableStreams: connected))
            #expect(connections.soonAvailableConnections == 4 - connected)
        }

        let requests3 = connections.refillConnections()
        #expect(requests3.isEmpty == true)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMakeConnectionLeaseItAndDropItHappyPath() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        #expect(connections.isEmpty)
        #expect(requests.isEmpty)

        guard let request = connections.createNewDemandConnectionIfPossible() else {
            Issue.record("Expected to receive a connection request")
            return
        }
        #expect(request == .init(connectionID: 0))
        #expect(!connections.isEmpty)
        #expect(connections.soonAvailableConnections == 1)
        #expect(connections.stats == .init(connecting: 1))

        let newConnection = MockConnection(id: request.connectionID)
        let (_, establishedContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        #expect(establishedContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(establishedContext.use == .demand)
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))
        #expect(connections.soonAvailableConnections == 0)

        guard case .leasedConnection(let leaseResult) = connections.leaseConnectionOrSoonAvailableConnectionCount() else {
            Issue.record("Expected to lease a connection")
            return
        }
        #expect(newConnection === leaseResult.connection)
        #expect(connections.stats == .init(leased: 1, leasedStreams: 1))

        guard let (index, releasedContext) = connections.releaseConnection(leaseResult.connection.id, streams: 1) else {
            Issue.record("Expected that this connection is still active")
            return
        }
        #expect(releasedContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(releasedContext.use == .demand)
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))

        let parkTimers = connections.parkConnection(at: index, hasBecomeIdle: true)
        #expect(parkTimers == [
            .init(timerID: 0, connectionID: newConnection.id, usecase: .keepAlive),
            .init(timerID: 1, connectionID: newConnection.id, usecase: .idleTimeout),
        ])

        guard let keepAliveAction = connections.keepAliveIfIdle(newConnection.id) else {
            Issue.record("Expected to get a connection for ping pong")
            return
        }
        #expect(newConnection === keepAliveAction.connection)
        #expect(connections.stats == .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, pingPongContext) = connections.keepAliveSucceeded(newConnection.id) else {
            Issue.record("Expected to get an AvailableContext")
            return
        }
        #expect(pingPongContext.info == .idle(availableStreams: 1, newIdle: false))
        #expect(releasedContext.use == .demand)
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))

        guard let closeAction = connections.closeConnectionIfIdle(newConnection.id) else {
            Issue.record("Expected to get a connection for ping pong")
            return
        }
        #expect(closeAction.timersToCancel == [])
        #expect(closeAction.connection === newConnection)
        #expect(connections.stats == .init(closing: 1, availableStreams: 0))

        let closeContext = connections.connectionClosed(newConnection.id, shuttingDown: false)
        #expect(closeContext.connectionsStarting == 0)
        #expect(connections.isEmpty)
        #expect(connections.stats == .init())
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testBackoffDoneCreatesANewConnectionToReachMinimumConnectionsEvenThoughRetryIsSetToFalse() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        #expect(connections.stats == .init(connecting: 1))
        #expect(connections.soonAvailableConnections == 1)
        #expect(!connections.isEmpty)
        #expect(requests.count == 1)

        guard let request = requests.first else {
            Issue.record("Expected to receive a connection request")
            return
        }
        #expect(request == .init(connectionID: 0))

        let backoffTimer = connections.backoffNextConnectionAttempt(request.connectionID)
        #expect(connections.stats == .init(backingOff: 1))
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        #expect(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken) == nil)

        let backoffDoneAction = connections.backoffDone(request.connectionID, retry: false)
        #expect(backoffDoneAction == .createConnection(.init(connectionID: 0), backoffTimerCancellationToken))

        #expect(connections.stats == .init(connecting: 1))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testBackoffDoneCancelsIdleTimerIfAPersistedConnectionIsNotRetried() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 2,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        #expect(connections.stats == .init(connecting: 2))
        #expect(connections.soonAvailableConnections == 2)
        #expect(!connections.isEmpty)
        #expect(requests.count == 2)

        var requestIterator = requests.makeIterator()
        guard let firstRequest = requestIterator.next(), let secondRequest = requestIterator.next() else {
            Issue.record("Expected to get two requests")
            return
        }

        guard let thirdRequest = connections.createNewDemandConnectionIfPossible() else {
            Issue.record("Expected to get another request")
            return
        }
        #expect(connections.stats == .init(connecting: 3))

        let newSecondConnection = MockConnection(id: secondRequest.connectionID)
        let (_, establishedSecondConnectionContext) = connections.newConnectionEstablished(newSecondConnection, maxStreams: 1)
        #expect(establishedSecondConnectionContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(establishedSecondConnectionContext.use == .persisted)
        #expect(connections.stats == .init(connecting: 2, idle: 1, availableStreams: 1))
        #expect(connections.soonAvailableConnections == 2)

        let newThirdConnection = MockConnection(id: thirdRequest.connectionID)
        let (thirdConnectionIndex, establishedThirdConnectionContext) = connections.newConnectionEstablished(newThirdConnection, maxStreams: 1)
        #expect(establishedThirdConnectionContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(establishedThirdConnectionContext.use == .demand)
        #expect(connections.stats == .init(connecting: 1, idle: 2, availableStreams: 2))
        #expect(connections.soonAvailableConnections == 1)
        let thirdConnKeepTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: thirdRequest.connectionID, usecase: .keepAlive)
        let thirdConnIdleTimer = TestPoolStateMachine.ConnectionTimer(timerID: 1, connectionID: thirdRequest.connectionID, usecase: .idleTimeout)
        let thirdConnIdleTimerCancellationToken = MockTimerCancellationToken(thirdConnIdleTimer)
        #expect(connections.parkConnection(at: thirdConnectionIndex, hasBecomeIdle: true) == [thirdConnKeepTimer, thirdConnIdleTimer])

        #expect(connections.timerScheduled(thirdConnKeepTimer, cancelContinuation: .init(thirdConnKeepTimer)) == nil)
        #expect(connections.timerScheduled(thirdConnIdleTimer, cancelContinuation: thirdConnIdleTimerCancellationToken) == nil)

        let backoffTimer = connections.backoffNextConnectionAttempt(firstRequest.connectionID)
        #expect(connections.stats == .init(backingOff: 1, idle: 2, availableStreams: 2))
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        #expect(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken) == nil)
        #expect(connections.stats == .init(backingOff: 1, idle: 2, availableStreams: 2))

        // connection three should be moved to connection one and for this reason become permanent

        #expect(connections.backoffDone(firstRequest.connectionID, retry: false) == .cancelTimers([backoffTimerCancellationToken, thirdConnIdleTimerCancellationToken]))
        #expect(connections.stats == .init(idle: 2, availableStreams: 2))

        #expect(connections.closeConnectionIfIdle(newThirdConnection.id) == nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testBackoffDoneReturnsNilIfOverflowConnection() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        guard let firstRequest = connections.createNewDemandConnectionIfPossible() else {
            Issue.record("Expected to get two requests")
            return
        }

        guard let secondRequest = connections.createNewDemandConnectionIfPossible() else {
            Issue.record("Expected to get another request")
            return
        }
        #expect(connections.stats == .init(connecting: 2))

        let newFirstConnection = MockConnection(id: firstRequest.connectionID)
        let (_, establishedFirstConnectionContext) = connections.newConnectionEstablished(newFirstConnection, maxStreams: 1)
        #expect(establishedFirstConnectionContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(establishedFirstConnectionContext.use == .demand)
        #expect(connections.stats == .init(connecting: 1, idle: 1, availableStreams: 1))
        #expect(connections.soonAvailableConnections == 1)

        let backoffTimer = connections.backoffNextConnectionAttempt(secondRequest.connectionID)
        let backoffTimerCancellationToken = MockTimerCancellationToken(backoffTimer)
        #expect(connections.stats == .init(backingOff: 1, idle: 1, availableStreams: 1))
        #expect(connections.timerScheduled(backoffTimer, cancelContinuation: backoffTimerCancellationToken) == nil)

        #expect(connections.backoffDone(secondRequest.connectionID, retry: false) == .cancelTimers([backoffTimerCancellationToken]))
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))

        #expect(connections.closeConnectionIfIdle(newFirstConnection.id) != nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testPingPong() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        let requests = connections.refillConnections()
        #expect(!connections.isEmpty)
        #expect(connections.stats == .init(connecting: 1))

        #expect(requests.count == 1)
        guard let firstRequest = requests.first else {
            Issue.record("Expected to have a request here")
            return
        }

        let newConnection = MockConnection(id: firstRequest.connectionID)
        let (connectionIndex, establishedConnectionContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        #expect(establishedConnectionContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(establishedConnectionContext.use == .persisted)
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))
        let timers = connections.parkConnection(at: connectionIndex, hasBecomeIdle: true)
        let keepAliveTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: firstRequest.connectionID, usecase: .keepAlive)
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(timers == [keepAliveTimer])
        #expect(connections.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        let keepAliveAction = connections.keepAliveIfIdle(newConnection.id)
        #expect(keepAliveAction == .init(connection: newConnection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken))
        #expect(connections.stats == .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, afterPingIdleContext) = connections.keepAliveSucceeded(newConnection.id) else {
            Issue.record("Expected to receive an AvailableContext")
            return
        }
        #expect(afterPingIdleContext.info == .idle(availableStreams: 1, newIdle: false))
        #expect(afterPingIdleContext.use == .persisted)
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveShouldNotIndicateCloseConnectionAfterClosed() {
        var connections = TestPoolStateMachine.ConnectionGroup(
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 2,
            maximumConcurrentConnectionHardLimit: 2,
            keepAlive: true,
            keepAliveReducesAvailableStreams: true
        )

        guard let firstRequest = connections.createNewDemandConnectionIfPossible() else {
            Issue.record("Expected to have a request here")
            return
        }

        let newConnection = MockConnection(id: firstRequest.connectionID)
        let (connectionIndex, establishedConnectionContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        #expect(establishedConnectionContext.info == .idle(availableStreams: 1, newIdle: true))
        #expect(connections.stats == .init(idle: 1, availableStreams: 1))
        _ = connections.parkConnection(at: connectionIndex, hasBecomeIdle: true)
        let keepAliveTimer = TestPoolStateMachine.ConnectionTimer(timerID: 0, connectionID: firstRequest.connectionID, usecase: .keepAlive)
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(connections.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        let keepAliveAction = connections.keepAliveIfIdle(newConnection.id)
        #expect(keepAliveAction == .init(connection: newConnection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken))
        #expect(connections.stats == .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        _ = connections.closeConnectionIfIdle(newConnection.id)
        guard connections.keepAliveFailed(newConnection.id) == nil else {
            Issue.record("Expected keepAliveFailed not to cause close again")
            return
        }
        #expect(connections.stats == .init(closing: 1))
    }
}
