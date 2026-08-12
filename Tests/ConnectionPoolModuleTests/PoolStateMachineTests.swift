@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest<MockConnection>,
    MockRequest<MockConnection>.ID,
    MockTimerCancellationToken,
    MockClock,
    MockClock.Instant
>

@Suite struct PoolStateMachineTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionsAreCreatedAndParkedOnStartup() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 2
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let connection1 = MockConnection(id: 0)
        let connection2 = MockConnection(id: 1)

        do {
            let requests = stateMachine.refillConnections()
            #expect(requests.count == 2)
            let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
            let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(10))
            let connection1KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer)
            #expect(createdAction1.request == .none)
            #expect(createdAction1.connection == .scheduleTimers([connection1KeepAliveTimer]))

            #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: connection1KeepAliveTimerCancellationToken) == .none)

            let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
            let connection2KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(10))
            let connection2KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer)
            #expect(createdAction2.request == .none)
            #expect(createdAction2.connection == .scheduleTimers([connection2KeepAliveTimer]))
            #expect(stateMachine.timerScheduled(connection2KeepAliveTimer, cancelContinuation: connection2KeepAliveTimerCancellationToken) == .none)
        }

        #expect(stateMachine.connections.stats.active == 2)
        #expect(stateMachine.connections.stats.idle == 2)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionsNoKeepAliveRun() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(5)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let connection1 = MockConnection(id: 0)

        // refill pool to at least one connection
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        #expect(createdAction1.request == .none)
        #expect(createdAction1.connection == .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        #expect(leaseRequest1.connection == .cancelTimers([]))
        #expect(leaseRequest1.request == .leaseConnection(.init(element: request1), connection1))

        // release connection 1
        #expect(stateMachine.releaseConnection(connection1, streams: 1) == .none())

        // lease connection 1
        let request2 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        #expect(leaseRequest2.connection == .cancelTimers([]))
        #expect(leaseRequest2.request == .leaseConnection(.init(element: request2), connection1))

        // request connection while none is available
        let request3 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest3 = stateMachine.leaseConnection(request3)
        #expect(leaseRequest3.connection == .makeConnection(.init(connectionID: 1), []))
        #expect(leaseRequest3.request == .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        #expect(createdAction2.request == .leaseConnection(.init(element: request3), connection2))
        #expect(createdAction2.connection == .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        #expect(
            stateMachine.releaseConnection(connection2, streams: 1) ==
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        #expect(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken) == .none)
        #expect(stateMachine.timerTriggered(connection2IdleTimer) == .init(request: .none, connection: .closeConnection(connection2, [connection2IdleTimerCancellationToken])))

        #expect(stateMachine.connections.stats.active == 1)
        #expect(stateMachine.connections.stats.leased == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testOnlyOverflowConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 0
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 0)

        // request connection while none exists
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        #expect(leaseRequest1.connection == .makeConnection(.init(connectionID: 0), []))
        #expect(leaseRequest1.request == .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        #expect(createdAction1.request == .leaseConnection(.init(element: request1), connection1))
        #expect(createdAction1.connection == .none)

        // request connection while none is available
        let request2 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        #expect(leaseRequest2.connection == .makeConnection(.init(connectionID: 1), []))
        #expect(leaseRequest2.request == .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        #expect(releaseRequest1.request == .leaseConnection(.init(element: request2), connection1))
        #expect(releaseRequest1.connection == .none)

        // connection 2 comes up and should be closed right away
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        #expect(createdAction2.request == .none)
        #expect(createdAction2.connection == .closeConnection(connection2, []))
        #expect(stateMachine.connectionClosed(connection2) == .none())

        // release connection 1 should be closed as well
        let releaseRequest2 = stateMachine.releaseConnection(connection1, streams: 1)
        #expect(releaseRequest2.request == .none)
        #expect(releaseRequest2.connection == .closeConnection(connection1, []))

        let shutdownAction = stateMachine.triggerForceShutdown()
        #expect(shutdownAction.request == .failRequests(.init(), .poolShutdown))
        #expect(shutdownAction.connection == .initiateShutdown(.init()))

        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testDemandConnectionIsMadePermanentIfPermanentIsClose() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 6
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let connection1 = MockConnection(id: 0)

        // refill pool to at least one connection
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        #expect(createdAction1.request == .none)
        #expect(createdAction1.connection == .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        #expect(leaseRequest1.connection == .cancelTimers([]))
        #expect(leaseRequest1.request == .leaseConnection(.init(element: request1), connection1))

        // request connection while none is available
        let request2 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        #expect(leaseRequest2.connection == .makeConnection(.init(connectionID: 1), []))
        #expect(leaseRequest2.request == .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        #expect(createdAction2.request == .leaseConnection(.init(element: request2), connection2))
        #expect(createdAction2.connection == .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        #expect(
            stateMachine.releaseConnection(connection2, streams: 1) ==
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        #expect(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken) == .none)

        // connection 1 is dropped
        #expect(stateMachine.connectionClosed(connection1) == .init(request: .none, connection: .cancelTimers([connection2IdleTimerCancellationToken])))

        #expect(stateMachine.connections.stats.active == 1)
        #expect(stateMachine.connections.stats.idle == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testReleaseLoosesRaceAgainstClosed() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 0)

        // request connection while none exists
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        #expect(leaseRequest1.connection == .makeConnection(.init(connectionID: 0), []))
        #expect(leaseRequest1.request == .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        #expect(createdAction1.request == .leaseConnection(.init(element: request1), connection1))
        #expect(createdAction1.connection == .none)

        // connection got closed
        let closedAction = stateMachine.connectionClosed(connection1)
        #expect(closedAction.connection == .none)
        #expect(closedAction.request == .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        #expect(releaseRequest1.request == .none)
        #expect(releaseRequest1.connection == .none)

        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveOnClosingConnection() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // don't refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 0)

        // request connection while none exists
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        #expect(leaseRequest1.connection == .makeConnection(.init(connectionID: 0), []))
        #expect(leaseRequest1.request == .none)

        // make connection 1
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        #expect(createdAction1.request == .leaseConnection(.init(element: request1), connection1))
        #expect(createdAction1.connection == .none)
        _ = stateMachine.releaseConnection(connection1, streams: 1)

        // trigger keep alive
        let keepAliveAction1 = stateMachine.connectionKeepAliveTimerTriggered(connection1.id)
        #expect(keepAliveAction1.connection == .runKeepAlive(connection1, nil))

        // fail keep alive and cause closed
        let keepAliveFailed1 = stateMachine.connectionKeepAliveFailed(connection1.id)
        #expect(keepAliveFailed1.connection == .closeConnection(connection1, []))
        connection1.closeIfClosing()

        // request connection while none exists anymore
        let request2 = MockRequest(connectionType: MockConnection.self)
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        #expect(leaseRequest2.connection == .makeConnection(.init(connectionID: 1), []))
        #expect(leaseRequest2.request == .none)

        // make connection 2
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        #expect(createdAction2.request == .leaseConnection(.init(element: request2), connection2))
        #expect(createdAction2.connection == .none)
        _ = stateMachine.releaseConnection(connection2, streams: 1)

        // trigger keep alive while connection is still open
        let keepAliveAction2 = stateMachine.connectionKeepAliveTimerTriggered(connection2.id)
        #expect(keepAliveAction2.connection == .runKeepAlive(connection2, nil))

        // close connection in the middle of keep alive
        connection2.close()
        connection2.closeIfClosing()

        // fail keep alive and cause closed
        let keepAliveFailed2 = stateMachine.connectionKeepAliveFailed(connection2.id)
        #expect(keepAliveFailed2.connection == .closeConnection(connection2, []))

        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionIsEstablishedAfterFailedKeepAliveIfNotEnoughConnectionsLeft() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // one connection should exist
        let request = MockRequest(connectionType: MockConnection.self)
        let leaseRequest = stateMachine.leaseConnection(request)
        #expect(leaseRequest.connection == .none)
        #expect(leaseRequest.request == .none)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .leaseConnection(.init(element: request), connection))
        #expect(createdAction.connection == .none)
        _ = stateMachine.releaseConnection(connection, streams: 1)

        // trigger keep alive
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(connection.id)
        #expect(keepAliveAction.connection == .runKeepAlive(connection, nil))

        // fail keep alive, cause closed and make new connection
        let keepAliveFailed = stateMachine.connectionKeepAliveFailed(connection.id)
        #expect(keepAliveFailed.connection == .closeConnection(connection, []))
        let connectionClosed = stateMachine.connectionClosed(connection)
        #expect(connectionClosed.connection == .makeConnection(.init(connectionID: 1), []))
        connection.closeIfClosing()
        let establishAction = stateMachine.connectionEstablished(.init(id: 1), maxStreams: 1)
        #expect(establishAction.request == .none)
        if case .scheduleTimers(let timers) = establishAction.connection {
            #expect(timers == [.init(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: configuration.keepAliveDuration!)])
        } else {
            Issue.record("Unexpected connection action")
        }

        #expect(stateMachine.connections.stats.active == 1)
        #expect(stateMachine.connections.stats.idle == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownWithIdleConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([connection1KeepAliveTimer]))
        #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: MockTimerCancellationToken(connection1KeepAliveTimer)) == .none)

        let shutdownAction = stateMachine.triggerForceShutdown()
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [connection]
        shutdown.timersToCancel = [MockTimerCancellationToken(connection1KeepAliveTimer)]
        #expect(shutdownAction.connection ==  .initiateShutdown(shutdown))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownWithLeasedConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([connection1KeepAliveTimer]))
        #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: MockTimerCancellationToken(connection1KeepAliveTimer)) == .none)

        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        #expect(leaseAction.request == .leaseConnection(.init(element: request), connection))
        #expect(leaseAction.connection == .cancelTimers([MockTimerCancellationToken(connection1KeepAliveTimer)]))

        let shutdownAction = stateMachine.triggerForceShutdown()
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [connection]
        #expect(shutdownAction.connection ==  .initiateShutdown(shutdown))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownWithInProgessRequest() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        let shutdownAction = stateMachine.triggerForceShutdown()
        #expect(shutdownAction.connection ==  .initiateShutdown(.init()))

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .closeConnection(connection, []))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownWithIdleConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([connection1KeepAliveTimer]))
        #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: MockTimerCancellationToken(connection1KeepAliveTimer)) == .none)

        let shutdownAction = stateMachine.triggerGracefulShutdown()
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [connection]
        shutdown.timersToCancel = [MockTimerCancellationToken(connection1KeepAliveTimer)]
        #expect(shutdownAction.connection == .initiateShutdown(shutdown))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownWithLeasedConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([connection1KeepAliveTimer]))
        #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: MockTimerCancellationToken(connection1KeepAliveTimer)) == .none)

        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        #expect(leaseAction.request == .leaseConnection(.init(element: request), connection))
        #expect(leaseAction.connection == .cancelTimers([MockTimerCancellationToken(connection1KeepAliveTimer)]))

        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [] // don't close connection yet since it's leased
        let shutdownAction = stateMachine.triggerGracefulShutdown()
        #expect(shutdownAction.connection == .initiateShutdown(shutdown))
        #expect(!stateMachine.isShutdown)
        
        let closeConnection = stateMachine.releaseConnection(connection, streams: 1)
        #expect(closeConnection.connection == .closeConnection(connection, []))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownOverridesRunningGracefulShutdown() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().count == 1)

        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        let keepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([keepAliveTimer]))
        #expect(stateMachine.timerScheduled(keepAliveTimer, cancelContinuation: MockTimerCancellationToken(keepAliveTimer)) == .none)

        let request = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(request).request == .leaseConnection(.init(element: request), connection))

        var gracefulShutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        gracefulShutdown.connections = [] // don't close connection yet since it's leased
        #expect(stateMachine.triggerGracefulShutdown().connection == .initiateShutdown(gracefulShutdown))
        #expect(!stateMachine.isShutdown)

        var forceShutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        forceShutdown.connections = [connection]
        #expect(stateMachine.triggerForceShutdown().connection == .initiateShutdown(forceShutdown))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownWithInProgessRequest() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [] // don't close connection yet since it's leased
        #expect(stateMachine.triggerGracefulShutdown().connection == .initiateShutdown(shutdown))

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .closeConnection(connection, []))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(.disabled("Pending https://github.com/vapor/postgres-nio/issues/657"))
    func testTriggerGracefulShutdownCreatesReplacementWhenConnectionDies() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // create connection and make it busy
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(mockRequest1)
        guard case .makeConnection = leaseAction.connection else {
            Issue.record()
            return
        }
        let connection = MockConnection(id: 0)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request == .leaseConnection(.init(element: mockRequest1), connection))

        // add request which will be enqueued
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest2) == .none())
        
        // trigger shutdown
        #expect(stateMachine.triggerGracefulShutdown() == .none())

        // kill existing connection and verify a new one is spun up
        let closeConnectionAction = stateMachine.connectionClosed(connection)
        #expect(closeConnectionAction.request == .none)
        guard case .makeConnection(let newConnectionRequest, _) = closeConnectionAction.connection else {
            Issue.record("New connection wasn't created even though there's work left to do")
            return
        }
        let mockConnection2 = MockConnection(id: newConnectionRequest.connectionID)
        #expect(stateMachine.connectionEstablished(mockConnection2, maxStreams: 1).request == .leaseConnection([mockRequest2], mockConnection2))
        #expect(stateMachine.connections.connections.count == 1)
        
        // complete work and verify shutdown
        #expect(stateMachine.releaseConnection(mockConnection2, streams: 1).connection == .closeConnection(mockConnection2, []))
        #expect(stateMachine.connectionClosed(mockConnection2).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownRejectsNewRequests() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: .init()
        )

        // create connection
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        let connection = MockConnection(id: 0)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request == .none)

        // make connection busy
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(mockRequest1)
        #expect(leaseAction.request == .leaseConnection(.init(element: mockRequest1), connection))
        #expect(leaseAction.connection == .cancelTimers([]))
        
        // trigger shutdown
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [] // don't close connection yet since it's leased
        #expect(stateMachine.triggerGracefulShutdown().connection == .initiateShutdown(shutdown))

        // verify new request is rejected
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest2).request == .failRequest(mockRequest2, .poolShutdown))
        
        // verify existing request completes and pool shuts down
        #expect(stateMachine.releaseConnection(connection, streams: 1).connection == .closeConnection(connection, []))
        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownDoesNotInterruptOtherStreams() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 2)
        #expect(createdAction.request == .none)
        let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.connection == .scheduleTimers([connection1KeepAliveTimer]))
        #expect(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: MockTimerCancellationToken(connection1KeepAliveTimer)) == .none)

        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction1 = stateMachine.leaseConnection(mockRequest1)
        #expect(leaseAction1.request == .leaseConnection(.init(element: mockRequest1), connection))
        #expect(leaseAction1.connection == .cancelTimers([MockTimerCancellationToken(connection1KeepAliveTimer)]))
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        let leaseAction2 = stateMachine.leaseConnection(mockRequest2)
        #expect(leaseAction2.request == .leaseConnection(.init(element: mockRequest2), connection))
        #expect(leaseAction2.connection == .cancelTimers([]))

        let release1 = stateMachine.releaseConnection(connection, streams: 1)
        #expect(release1.connection == .none)

        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [] // don't close connection yet since it's leased
        #expect(stateMachine.triggerGracefulShutdown().connection == .initiateShutdown(shutdown))
        #expect(!stateMachine.isShutdown)

        let release2 = stateMachine.releaseConnection(connection, streams: 1)
        #expect(release2.connection == .closeConnection(connection, []))

        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownDrainsQueueWithoutCreatingConnections() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(mockRequest1).connection else { Issue.record(); return }
        let connection = MockConnection(id: 0)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request == .leaseConnection(.init(element: mockRequest1), connection))

        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest2) == .none())
        let mockRequest3 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest3) == .none())

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        let drain1 = stateMachine.releaseConnection(connection, streams: 1)
        #expect(drain1.request == .leaseConnection(.init(element: mockRequest2), connection))
        if case .makeConnection = drain1.connection { Issue.record("graceful shutdown must not create connections") }

        let drain2 = stateMachine.releaseConnection(connection, streams: 1)
        #expect(drain2.request == .leaseConnection(.init(element: mockRequest3), connection))
        if case .makeConnection = drain2.connection { Issue.record("graceful shutdown must not create connections") }

        #expect(stateMachine.releaseConnection(connection, streams: 1).connection == .closeConnection(connection, []))
        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownKeepsDrainingQueueAfterOneConnectionCloses() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(mockRequest1).connection else { Issue.record(); return }
        let connection1 = MockConnection(id: 0)
        #expect(stateMachine.connectionEstablished(connection1, maxStreams: 1).request == .leaseConnection(.init(element: mockRequest1), connection1))

        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(mockRequest2).connection else { Issue.record(); return }
        let connection2 = MockConnection(id: 1)
        #expect(stateMachine.connectionEstablished(connection2, maxStreams: 1).request == .leaseConnection(.init(element: mockRequest2), connection2))

        let mockRequest3 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest3) == .none())
        let mockRequest4 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(mockRequest4) == .none())

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        let closedAction = stateMachine.connectionClosed(connection1)
        #expect(closedAction.request == .none)
        #expect(closedAction.connection == .cancelTimers([]))
        #expect(!stateMachine.isShutdown)

        // creates a new connection to help drain
        let drain1 = stateMachine.releaseConnection(connection2, streams: 1)
        #expect(drain1.request == .leaseConnection(.init(element: mockRequest3), connection2))
        guard 
            case .makeConnectionsCancelAndScheduleTimers(let requests, _, _) = drain1.connection,
            let newConnectionRequest = Array(requests).first
        else {
            Issue.record("Expected a connection to be created to help drain the queue, got \(drain1.connection)")
            return
        }
        
        // release manually created connections but is not ready to close because of auto-created third one
        #expect(stateMachine.releaseConnection(connection2, streams: 1).request == .leaseConnection(.init(element: mockRequest4), connection2))
        #expect(stateMachine.releaseConnection(connection2, streams: 1).connection == .closeConnection(connection2, []))
        #expect(stateMachine.connectionClosed(connection2).connection == .cancelTimers([]))

        // close auto-created third connection and shut down
        let connection3 = MockConnection(id: newConnectionRequest.connectionID)
        let established3 = stateMachine.connectionEstablished(connection3, maxStreams: 1)
        #expect(established3.request == .none)
        #expect(established3.connection == .closeConnection(connection3, []))
        #expect(stateMachine.connectionClosed(connection3).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownReapsBackingOffConnection() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])

        _ = stateMachine.triggerGracefulShutdown()
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownKeepsRetryingWhenConnectionCreationFails() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        // The request triggers a connection creation and is queued until the connection is up.
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest, _) = stateMachine.leaseConnection(mockRequest1).connection else {
            Issue.record()
            return
        }

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // connection failed: keep trying until either a force shutdown is triggered, or a connection succeeds
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest)
        #expect(failedAction.request == .none)
        guard case .scheduleTimers(let timers) = failedAction.connection, let backoffTimer = Array(timers).first else {
            Issue.record("Expected a backoff timer to be scheduled, got \(failedAction.connection)")
            return
        }
        #expect(!stateMachine.isShutdown)
        let backoffTimerToken = MockTimerCancellationToken(backoffTimer)
        #expect(stateMachine.timerScheduled(backoffTimer, cancelContinuation: backoffTimerToken) == .none)

        // connection is up again, use it
        let retryAction = stateMachine.timerTriggered(backoffTimer)
        #expect(retryAction.request == .none)
        guard case .makeConnection(let retryRequest, let cancelledTimers) = retryAction.connection else {
            Issue.record("Expected a retry connection attempt, got \(retryAction.connection)")
            return
        }
        #expect(Array(cancelledTimers) == [backoffTimerToken])

        let connection = MockConnection(id: retryRequest.connectionID)
        let establishedAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(establishedAction.request == .leaseConnection(.init(element: mockRequest1), connection))
        #expect(establishedAction.connection == .none)

        #expect(stateMachine.releaseConnection(connection, streams: 1).connection == .closeConnection(connection, []))
        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownKeepsRetryingUntilForceShutdown() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        // connection requested but fails
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest, _) = stateMachine.leaseConnection(mockRequest1).connection else {
            Issue.record()
            return
        }
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest)
        guard case .scheduleTimers(let timers) = failedAction.connection, let backoffTimer = Array(timers).first else {
            Issue.record()
            return
        }
        let backoffTimerToken = MockTimerCancellationToken(backoffTimer)
        #expect(stateMachine.timerScheduled(backoffTimer, cancelContinuation: backoffTimerToken) == .none)

        // graceful shutdown should keep connection alive and retrying
        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // new requests are rejected while the drain is in progress
        let lateRequest = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(lateRequest).request == .failRequest(lateRequest, .poolShutdown))

        // only force shutdown can complete the shutdown
        let forceAction = stateMachine.triggerForceShutdown()
        #expect(forceAction.request == .failRequests(.init(element: mockRequest1), .poolShutdown))
        #expect(forceAction.connection == .cancelEventStreamAndFinalCleanup([backoffTimerToken]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownCompletesAfterLastQueuedRequestIsCancelled() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // one connection for two requests
        let request1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(request1).connection else { Issue.record(); return }
        let connection = MockConnection(id: 0)
        #expect(stateMachine.connectionEstablished(connection, maxStreams: 1).request == .leaseConnection(.init(element: request1), connection))
        let request2 = MockRequest(connectionType: MockConnection.self)
        #expect(stateMachine.leaseConnection(request2) == .none())

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // cancel one request
        let cancelAction = stateMachine.cancelRequest(id: request2.id)
        #expect(cancelAction.request == .failRequest(request2, .requestCancelled))
        #expect(cancelAction.connection == .none)
        #expect(!stateMachine.isShutdown)

        #expect(stateMachine.releaseConnection(connection, streams: 1).connection == .closeConnection(connection, []))
        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testCircuitBreakerTripDuringGracefulShutdownDrainsQueueAndCompletes() {
        struct ConnectionFailed: Error {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // connection creation fails: connectionCreationFailing state
        let request1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest, _) = stateMachine.leaseConnection(request1).connection else { Issue.record(); return }
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest)
        guard case .scheduleTimers(let timers1) = failedAction.connection, let backoffTimer1 = Array(timers1).first else { Issue.record(); return }
        let backoffToken1 = MockTimerCancellationToken(backoffTimer1)
        #expect(stateMachine.timerScheduled(backoffTimer1, cancelContinuation: backoffToken1) == .none)

        // graceful shutdown does not shutdown immediately
        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // circuit breaker trips, the requests fail so we should go directly into shut down
        clock.advance(to: clock.now.advanced(by: .seconds(16)))
        let retryAction = stateMachine.timerTriggered(backoffTimer1)
        guard case .makeConnection(let retryRequest, _) = retryAction.connection else { Issue.record(); return }
        let trippedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: retryRequest)
        #expect(trippedAction.request == .failRequests(.init(element: request1), .connectionCreationCircuitBreakerTripped))
        #expect(trippedAction.connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownWhileCircuitBreakerIsOpen() {
        struct ConnectionFailed: Error {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.circuitBreakerTripAfter = .seconds(15)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // trip the circuit breaker
        let request1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest, _) = stateMachine.leaseConnection(request1).connection else { Issue.record(); return }
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest)
        guard case .scheduleTimers(let timers1) = failedAction.connection, let backoffTimer1 = Array(timers1).first else { Issue.record(); return }
        #expect(stateMachine.timerScheduled(backoffTimer1, cancelContinuation: MockTimerCancellationToken(backoffTimer1)) == .none)

        clock.advance(to: clock.now.advanced(by: .seconds(16)))
        let retryAction = stateMachine.timerTriggered(backoffTimer1)
        guard case .makeConnection(let retryRequest, _) = retryAction.connection else { Issue.record(); return }
        let trippedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: retryRequest)
        #expect(trippedAction.request == .failRequests(.init(element: request1), .connectionCreationCircuitBreakerTripped))
        guard case .scheduleTimers(let timers2) = trippedAction.connection, let backoffTimer2 = Array(timers2).first else { Issue.record(); return }
        let backoffToken2 = MockTimerCancellationToken(backoffTimer2)
        #expect(stateMachine.timerScheduled(backoffTimer2, cancelContinuation: backoffToken2) == .none)
        #expect(!stateMachine.isShutdown)

        // graceful shutdown should complete immediately
        let shutdownAction = stateMachine.triggerGracefulShutdown()
        #expect(shutdownAction.request == .none)
        #expect(shutdownAction.connection == .cancelEventStreamAndFinalCleanup([backoffToken2]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testCircuitBreakerTripDuringGracefulShutdownWaitsForStartingConnection() {
        struct ConnectionFailed: Error {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.circuitBreakerTripAfter = .seconds(15)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // two inflight connections
        let request1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest1, _) = stateMachine.leaseConnection(request1).connection else { Issue.record(); return }
        let request2 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest2, _) = stateMachine.leaseConnection(request2).connection else { Issue.record(); return }

        // first one fails, second one is starting
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest1)
        guard case .scheduleTimers(let timers1) = failedAction.connection, let backoffTimer1 = Array(timers1).first else { Issue.record(); return }
        #expect(stateMachine.timerScheduled(backoffTimer1, cancelContinuation: MockTimerCancellationToken(backoffTimer1)) == .none)

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // circuit breaker trips with starting connections
        clock.advance(to: clock.now.advanced(by: .seconds(16)))
        let retryAction = stateMachine.timerTriggered(backoffTimer1)
        guard case .makeConnection(let retryRequest, _) = retryAction.connection else { Issue.record(); return }
        let trippedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: retryRequest)
        #expect(trippedAction.request == .failRequests(.init([request1, request2]), .connectionCreationCircuitBreakerTripped))
        #expect(trippedAction.connection == .cancelTimers([]))
        #expect(!stateMachine.isShutdown)

        // complete the shutdown once the starting connection is closed
        let connection2 = MockConnection(id: connectionRequest2.connectionID)
        let establishedAction = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        #expect(establishedAction.request == .none)
        #expect(establishedAction.connection == .closeConnection(connection2, []))

        #expect(stateMachine.connectionClosed(connection2).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testGracefulShutdownCleansUpBackingOffConnectionAfterQueueDrains() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // connectionCreationFailing
        let request1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest1, _) = stateMachine.leaseConnection(request1).connection else { Issue.record(); return }
        let connection1 = MockConnection(id: connectionRequest1.connectionID)
        #expect(stateMachine.connectionEstablished(connection1, maxStreams: 1).request == .leaseConnection(.init(element: request1), connection1))

        let request2 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection(let connectionRequest2, _) = stateMachine.leaseConnection(request2).connection else { Issue.record(); return }
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: connectionRequest2)
        guard case .scheduleTimers(let timers) = failedAction.connection, let backoffTimer = Array(timers).first else { Issue.record(); return }
        let backoffToken = MockTimerCancellationToken(backoffTimer)
        #expect(stateMachine.timerScheduled(backoffTimer, cancelContinuation: backoffToken) == .none)

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        // connection 1 drains the queue
        #expect(stateMachine.releaseConnection(connection1, streams: 1).request == .leaseConnection(.init(element: request2), connection1))
        #expect(stateMachine.releaseConnection(connection1, streams: 1).connection == .closeConnection(connection1, []))
        let closedAction = stateMachine.connectionClosed(connection1)
        #expect(closedAction.request == .none)
        #expect(!stateMachine.isShutdown)

        // the backing off connection is all that remains, assert we close it and shut down
        let backoffDoneAction = stateMachine.timerTriggered(backoffTimer)
        #expect(backoffDoneAction.request == .none)
        #expect(backoffDoneAction.connection == .cancelEventStreamAndFinalCleanup([backoffToken]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownCompletesWhenStartingConnectionFailsWithNoQueuedRequests() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = []
        #expect(stateMachine.triggerGracefulShutdown().connection == .initiateShutdown(shutdown))
        #expect(!stateMachine.isShutdown)

        // no requests to complete so the connection gets closed
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])
        #expect(failedAction.request == .none)
        #expect(failedAction.connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerGracefulShutdownWithStartingConnectionDrainsQueueOnceEstablished() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(mockRequest1).connection else { Issue.record(); return }

        #expect(stateMachine.triggerGracefulShutdown() == .none())
        #expect(!stateMachine.isShutdown)

        let establishedAction = stateMachine.connectionEstablished(MockConnection(id: 0), maxStreams: 1)
        guard case .leaseConnection(let requests, let connection) = establishedAction.request else {
            Issue.record("Expected the queued request to be leased, got \(establishedAction.request)")
            return
        }
        #expect(Array(requests) == [mockRequest1])

        #expect(stateMachine.releaseConnection(connection, streams: 1).connection == .closeConnection(connection, []))
        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownWithStartingConnection() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil
        configuration.idleTimeoutDuration = .seconds(3)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        #expect(stateMachine.refillConnections().isEmpty)

        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        guard case .makeConnection = stateMachine.leaseConnection(mockRequest1).connection else { Issue.record(); return }

        let shutdownAction = stateMachine.triggerForceShutdown()
        #expect(shutdownAction.request == .failRequests(.init(element: mockRequest1), .poolShutdown))
        #expect(!stateMachine.isShutdown)

        let connection = MockConnection(id: 0)
        let establishedAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(establishedAction.request == .none)
        #expect(establishedAction.connection == .closeConnection(connection, []))

        #expect(stateMachine.connectionClosed(connection).connection == .cancelEventStreamAndFinalCleanup([]))
        #expect(stateMachine.isShutdown)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testBackingOffRequests() {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)
        configuration.circuitBreakerTripAfter = .seconds(30)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // request two connections
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction1 = stateMachine.leaseConnection(mockRequest1)
        guard case .makeConnection(let request1, _) = leaseAction1.connection else {
            Issue.record()
            return
        }
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        let leaseAction2 = stateMachine.leaseConnection(mockRequest2)
        guard case .makeConnection(let request2, _) = leaseAction2.connection else {
            Issue.record()
            return
        }

        // fail connection 1
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: request1)
        #expect(failedAction.request == .none)
        switch failedAction.connection {
        case .scheduleTimers(let timers):
            #expect(timers.count == 1)
            #expect(timers.first?.underlying.usecase == .backoff)
        default:
            Issue.record()
        }

        let mockRequest3 = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(mockRequest3)
        #expect(leaseAction.request == .none)
        #expect(leaseAction.connection == .none)

        clock.advance(to: clock.now.advanced(by: .seconds(31)))

        // fail connection 2. Connection request is removed as we already have a failing connection
        let failedAction2 = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: request2)
        #expect(failedAction2.request == .none)
        #expect(failedAction2.connection == .cancelTimers(.init()))
        #expect(stateMachine.connections.connections.count == 1)

        let backOffDone = stateMachine.connectionCreationBackoffDone(request1.connectionID)
        #expect(backOffDone.request == .none)
        #expect(backOffDone.connection == .makeConnection(request1, []))

        // fail connection 1 again
        let failedAction3 = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: request1)
        switch failedAction3.request {
        case .failRequests(let requests, let error):
            #expect(Set(requests) == Set([mockRequest1, mockRequest2, mockRequest3]))
            #expect(error == ConnectionPoolError.connectionCreationCircuitBreakerTripped)
        default:
            Issue.record()
        }
        switch failedAction3.connection {
        case .scheduleTimers(let timers):
            #expect(timers.count == 1)
            #expect(timers.first?.underlying.usecase == .backoff)
        default:
            Issue.record()
        }

        // lease fails immediately as we are in circuitBreak state
        let request3 = MockRequest(connectionType: MockConnection.self)
        let leaseAction3 = stateMachine.leaseConnection(request3)
        #expect(leaseAction3.request == .failRequest(request3, ConnectionPoolError.connectionCreationCircuitBreakerTripped))
        #expect(leaseAction3.connection == .none)

        let backOffDone2 = stateMachine.connectionCreationBackoffDone(request1.connectionID)
        #expect(backOffDone2.request == .none)
        #expect(backOffDone2.connection == .makeConnection(request1, []))

        // make connection
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        let connectionKeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        let connectionIdleTimer = TestPoolStateMachine.Timer(.init(timerID: 3, connectionID: 0, usecase: .idleTimeout), duration: .seconds(4))
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .scheduleTimers([connectionKeepAliveTimer, connectionIdleTimer]))

        // lease connection (successful)
        let request4 = MockRequest(connectionType: MockConnection.self)
        let leaseAction4 = stateMachine.leaseConnection(request4)
        #expect(leaseAction4.request == .leaseConnection(.init(element: request4), connection))
        #expect(leaseAction4.connection == .none)

        #expect(stateMachine.connections.stats.leased == 1)
        #expect(stateMachine.connections.stats.active == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTwoConnectionsFailAndSucceed() throws {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 10
        configuration.maximumConnectionHardLimit = 10
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)
        configuration.maximumConcurrentConnectionRequests = 3

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // request two connections
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction1 = stateMachine.leaseConnection(mockRequest1)
        guard case .makeConnection(let request1, _) = leaseAction1.connection else {
            Issue.record()
            return
        }
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        let leaseAction2 = stateMachine.leaseConnection(mockRequest2)
        guard case .makeConnection = leaseAction2.connection else {
            Issue.record()
            return
        }

        // fail connection 1
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: request1)
        #expect(failedAction.request == .none)
        guard case .scheduleTimers(let timers) = failedAction.connection else { 
            Issue.record()
            return
        }
        #expect(timers.count == 1)
        let timer = try #require(timers.first)
        #expect(timer.underlying.usecase == .backoff)

        // make connection
        let connection = MockConnection(id: 1)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .leaseConnection([mockRequest1], connection))

        // backoff timer from failed connection triggers
        let timerAction = stateMachine.timerTriggered(timer)
        #expect(timerAction.connection == .makeConnection(request1, []))
    }

    /// Test that we limit concurrent connection requests and that when connections are established
    /// we request new connections
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConcurrentConnectionRequestsLimit() {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 10
        configuration.maximumConnectionHardLimit = 10
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)
        configuration.maximumConcurrentConnectionRequests = 3

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )
        let requests = (0..<5).map { _ in MockRequest(connectionType: MockConnection.self) }
        let leaseRequests = requests.map { stateMachine.leaseConnection($0) }
        #expect(leaseRequests[0].connection == .makeConnection(.init(connectionID: 0), []))
        #expect(leaseRequests[1].connection == .makeConnection(.init(connectionID: 1), []))
        #expect(leaseRequests[2].connection == .makeConnection(.init(connectionID: 2), []))
        #expect(leaseRequests[3].connection == .none)
        #expect(leaseRequests[4].connection == .none)
        for i in 0..<5 {
            #expect(leaseRequests[i].request == .none)
        }

        // only 4 connections are created as once 4 are created we have enough streams and connections available to serve
        // all requests
        let connections = (0..<4).map { MockConnection(id: $0) }
        let connectedActions = (0..<4).map { stateMachine.connectionEstablished(connections[$0], maxStreams: 1) }
        #expect(connectedActions[0].connection == .makeConnectionsCancelAndScheduleTimers(.init(element: .init(connectionID: 3)), [], []))
        #expect(connectedActions[1].connection == .cancelTimers([]))
        #expect(connectedActions[2].connection == .cancelTimers([]))
        #expect(connectedActions[3].connection == .cancelTimers([]))
        for i in 0..<4 {
            #expect(connectedActions[i].request == .leaseConnection([requests[i]], connections[i]))
        }
        let releaseActions = (0..<4).map { stateMachine.releaseConnection(connections[$0], streams: 1)}
        #expect(releaseActions[0].request == .leaseConnection([requests[4]], connections[0]))
        #expect(releaseActions[1].request == .none)
        #expect(releaseActions[2].request == .none)
        #expect(releaseActions[3].request == .none)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRefillConnectionPoolAfterConnectionFail() {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 5
        configuration.maximumConnectionSoftLimit = 10
        configuration.maximumConnectionHardLimit = 10
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)
        configuration.maximumConcurrentConnectionRequests = 3

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 5)

        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[1])
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[2])
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[3])
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[4])

        let backOffDone2 = stateMachine.connectionCreationBackoffDone(requests[0].connectionID)
        #expect(backOffDone2.request == .none)
        #expect(backOffDone2.connection == .makeConnection(requests[0], []))

        // make connection. Should return request to create 3 new connections
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        let newRequests = (5..<8).map { TestPoolStateMachine.ConnectionRequest(connectionID: $0) }
        let connectionKeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .makeConnectionsCancelAndScheduleTimers(.init(newRequests), [], .init(connectionKeepAliveTimer)))

        // make connection. Return 
        let connection2 = MockConnection(id: 5)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        let connectionKeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 5, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction2.request == .none)
        #expect(createdAction2.connection == .makeConnectionsCancelAndScheduleTimers(
            .init(element: TestPoolStateMachine.ConnectionRequest(connectionID: 8)), [], .init(connectionKeepAliveTimer2))
        )

        #expect(stateMachine.connections.stats.active == 5)
        #expect(stateMachine.connections.stats.idle == 2)
        #expect(stateMachine.connections.stats.connecting == 3)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMultipleBackingOffConnections() {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 3
        configuration.maximumConnectionSoftLimit = 10
        configuration.maximumConnectionHardLimit = 10
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)
        configuration.maximumConcurrentConnectionRequests = 3

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: clock
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 3)

        // Connection failed go into connectionCreationFailing state
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])
        let connection1 = MockConnection(id: 1)
        // Connection was successful go back to running state
        _ = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        // Connection failed go into connectionCreationFailing state
        _ = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[2])

        // backoff timer requested by first connectionEstablishFailed is done. It is not the
        // connection stored in the failing state context so just cancel it related timers
        let backOffDone = stateMachine.connectionCreationBackoffDone(requests[0].connectionID)
        #expect(backOffDone.request == .none)
        #expect(backOffDone.connection == .cancelTimers([]))

        // backoff timer requested by second connectionEstablishFailed is done, try to create a
        // connection again
        let backOffDone2 = stateMachine.connectionCreationBackoffDone(requests[2].connectionID)
        #expect(backOffDone2.request == .none)
        #expect(backOffDone2.connection == .makeConnection(requests[2], []))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testTriggerForceShutdownWithBackingOffRequest() {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 2
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 2)

        // Add two connections to verify we don't use an out of bounds index when iterating the 
        // connection array on triggerForceShutdown. The first connection will be deleted as it
        // never connected. Need to be sure when we access the second connection it is with the
        // correct index

        // fail connection 1
        let failedAction = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])
        #expect(failedAction.request == .none)
        switch failedAction.connection {
        case .scheduleTimers(let timers):
            #expect(timers.count == 1)
            #expect(timers.first?.underlying.usecase == .backoff)
        default:
            Issue.record()
        }

        // make connection 2
        let connection2 = MockConnection(id: 1)
        let createdAction = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        let connection2KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .scheduleTimers([connection2KeepAliveTimer]))

        let shutdownAction = stateMachine.triggerForceShutdown()
        var shutdown = TestPoolStateMachine.ConnectionAction.Shutdown()
        shutdown.connections = [connection2]
        #expect(shutdownAction.connection ==  .initiateShutdown(shutdown))

        let closedAction = stateMachine.connectionClosed(connection2)
        #expect(closedAction.connection == .cancelEventStreamAndFinalCleanup([]))

        #expect(stateMachine.isShutdown)
        #expect(stateMachine.connections.stats.active == 0)
    }

    /// Test that keep alive, idle timer triggering and leasing a connection do not race
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveIdleTimerTriggerRaceCondition() throws {
        struct ConnectionFailed: Error {}
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // lease connection
        let mockRequest1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction1 = stateMachine.leaseConnection(mockRequest1)
        guard case .makeConnection(let request1, _) = leaseAction1.connection else {
            Issue.record()
            return
        }
        // establish connection
        let establishConnection1 = stateMachine.connectionEstablished(.init(id: request1.connectionID), maxStreams: 1)
        guard case .cancelTimers(let timers1) = establishConnection1.connection else {
            Issue.record()
            return
        }
        // release connection
        #expect(timers1.count == 0)
        let releaseConnection1 = stateMachine.releaseConnection(.init(id: request1.connectionID), streams: 1)
        guard case .scheduleTimers(let timers2) = releaseConnection1.connection else {
            Issue.record()
            return
        }
        let keepAliveTimer = try #require(timers2.first)
        let idleTimeoutTimer = try #require(timers2.second)
        #expect(keepAliveTimer.underlying.usecase == .keepAlive) 
        #expect(idleTimeoutTimer.underlying.usecase == .idleTimeout)
        // trigger keep alive
        let timerTriggered = stateMachine.timerTriggered(keepAliveTimer)
        guard case .runKeepAlive = timerTriggered.connection else {
            Issue.record()
            return
        }
        // lease connection
        let mockRequest2 = MockRequest(connectionType: MockConnection.self)
        let leaseAction2 = stateMachine.leaseConnection(mockRequest2)
        guard case .none = leaseAction2.connection else {
            Issue.record()
            return
        }
        // trigger timeout timer
        let idleTimerTriggered = stateMachine.timerTriggered(idleTimeoutTimer)
        guard case .scheduleTimers(let idleTimersRescheduled) = idleTimerTriggered.connection else {
            Issue.record()
            return
        }
        #expect(idleTimersRescheduled.first?.underlying.usecase == .idleTimeout)

        // keepalive done
        let keepAliveDone = stateMachine.connectionKeepAliveDone(MockConnection(id: 0))
        // would expect a makeConnection as we need a new connection to serve the lease request
        guard case .leaseConnection(_, _) = keepAliveDone.request else {
            Issue.record()
            return
        }
    }

    // Regression test: keep-alive timer fires while requestQueue is non-empty.
    // This reproduces a crash (precondition failure at PoolStateMachine.swift:561)
    // when PG restarts: connection creation fails, new lease requests get queued,
    // but keep-alive timers on surviving idle connections are still armed.
    // Mirrors the fix in connectionIdleTimerTriggered (vapor/postgres-nio PR #627).
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveTimerWithQueuedRequests() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 2
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // 1. Create two connections (the pool minimum)
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 2)

        let connection0 = MockConnection(id: 0)
        let connection1 = MockConnection(id: 1)

        let created0 = stateMachine.connectionEstablished(connection0, maxStreams: 1)
        let timer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(10))
        let cancel0 = MockTimerCancellationToken(timer0)
        #expect(created0.connection == .scheduleTimers([timer0]))
        #expect(stateMachine.timerScheduled(timer0, cancelContinuation: cancel0) == .none)

        let created1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        let timer1 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(10))
        let cancel1 = MockTimerCancellationToken(timer1)
        #expect(created1.connection == .scheduleTimers([timer1]))
        #expect(stateMachine.timerScheduled(timer1, cancelContinuation: cancel1) == .none)

        // Both connections are idle with keep-alive timers armed
        #expect(stateMachine.connections.stats.idle == 2)

        // 2. Close connection 1 — pool will try to create a replacement
        connection1.close()
        let closedAction = stateMachine.connectionClosed(connection1)
        // Pool should request a new connection to maintain minimum.
        // Only connection 1's keep-alive timer is cancelled (connection 0 is still alive).
        #expect(closedAction.connection == .makeConnection(.init(connectionID: 2), [cancel1]))

        // 3. The replacement connection (id=2) fails to connect (PG is down)
        let failAction = stateMachine.connectionEstablishFailed(
            SomeError(),
            for: .init(connectionID: 2)
        )
        // Pool enters connectionCreationFailing state and schedules a backoff timer
        #expect(failAction.request == .none)

        // 4. A lease request arrives — gets queued because pool is in connectionCreationFailing
        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        #expect(leaseAction.request == .none)  // queued, not served
        #expect(leaseAction.connection == .none)

        // 5. Keep-alive timer fires on connection 0 (still idle).
        //    BUG: precondition(self.requestQueue.isEmpty) crashes here.
        //    FIX: the guard on keepAliveIfIdle handles this safely.
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(connection0.id)
        #expect(keepAliveAction.connection == .runKeepAlive(connection0, cancel0))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionWillCloseOnIdleConnectionClosesAndReplacesIfPersisted() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 4
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)
        // Park the connection
        guard case .scheduleTimers(let timers) = createdAction.connection else {
            Issue.record("Expected scheduleTimers")
            return
        }
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keep alive timer")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(stateMachine.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        // connectionWillClose on idle connection → should close
        let willCloseAction = stateMachine.connectionWillClose(0)
        #expect(willCloseAction.request == .none)
        guard case .closeConnection(let closedConn, _) = willCloseAction.connection else {
            Issue.record("Expected closeConnection action")
            return
        }
        #expect(closedConn === connection)

        // Now the connection actually closes → should create a replacement (persisted)
        let closedAction = stateMachine.connectionClosed(connection)
        #expect(closedAction.request == .none)
        guard case .makeConnection(let newRequest, _) = closedAction.connection else {
            Issue.record("Expected makeConnection for replacement")
            return
        }
        #expect(newRequest.connectionID == 1) // new connection ID
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionWillCloseOnLeasedDrainsAndCloses() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 4
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(createdAction.request == .none)

        // Lease the connection
        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        guard case .leaseConnection(_, let leasedConn) = leaseAction.request else {
            Issue.record("Expected leaseConnection")
            return
        }
        #expect(leasedConn === connection)

        // connectionWillClose on leased connection → no immediate action
        let willCloseAction = stateMachine.connectionWillClose(0)
        #expect(willCloseAction == .none())

        // Release → should trigger close
        let releaseAction = stateMachine.releaseConnection(connection, streams: 1)
        #expect(releaseAction.request == .none)
        guard case .closeConnection(let closedConn, _) = releaseAction.connection else {
            Issue.record("Expected closeConnection on release after connectionWillClose")
            return
        }
        #expect(closedConn === connection)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testConnectionWillCloseOnLeasedWithKeepAlive_ClosesOnRelease() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 4
        configuration.maximumConnectionHardLimit = 4
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)

        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 100)
        #expect(createdAction.request == .none)
        guard case .scheduleTimers(let timers) = createdAction.connection else {
            Issue.record("Expected scheduleTimers")
            return
        }
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keep alive timer")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(stateMachine.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        // Trigger keepAlive
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(0)
        guard case .runKeepAlive(let kaConnection, _) = keepAliveAction.connection else {
            Issue.record("Expected runKeepAlive")
            return
        }
        #expect(kaConnection === connection)

        // Lease while keepAlive is running
        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        guard case .leaseConnection(_, let leasedConn) = leaseAction.request else {
            Issue.record("Expected leaseConnection")
            return
        }
        #expect(leasedConn === connection)

        // connectionWillClose while leased with keepAlive running → no immediate action
        let willCloseAction = stateMachine.connectionWillClose(0)
        #expect(willCloseAction == .none())

        // Release → should trigger close immediately (don't wait for keepAlive)
        let releaseAction = stateMachine.releaseConnection(connection, streams: 1)
        #expect(releaseAction.request == .none)
        guard case .closeConnection(let closedConn, _) = releaseAction.connection else {
            Issue.record("Expected closeConnection on release after connectionWillClose")
            return
        }
        #expect(closedConn === connection)
    }

    // MARK: - connectionReceivedNewMaxStreamSetting tests

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func increaseMaxStreamsWithQueuedRequests() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let refillRequests = stateMachine.refillConnections()
        #expect(refillRequests.count == 1)
        let connection = MockConnection(id: 0)
        _ = stateMachine.connectionEstablished(connection, maxStreams: 1)

        // Lease the only available stream
        let request1 = MockRequest(connectionType: MockConnection.self)
        let lease1 = stateMachine.leaseConnection(request1)
        #expect(lease1.request == .leaseConnection(.init(element: request1), connection))

        // Queue additional requests
        let request2 = MockRequest(connectionType: MockConnection.self)
        let lease2 = stateMachine.leaseConnection(request2)
        #expect(lease2.request == .none) // queued

        let request3 = MockRequest(connectionType: MockConnection.self)
        let lease3 = stateMachine.leaseConnection(request3)
        #expect(lease3.request == .none) // queued

        // Increase maxStreams — should dequeue waiting requests
        let action = stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: 4)
        guard case .leaseConnection(let requests, let leasedConn) = action.request else {
            Issue.record("Expected leaseConnection action")
            return
        }
        #expect(leasedConn === connection)
        // Should lease min(increase=3, capacity=3, waiting=2) = 2 requests
        #expect(requests.count == 2)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func increaseMaxStreamsWithNoQueuedRequests() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let refillRequests = stateMachine.refillConnections()
        #expect(refillRequests.count == 1)
        let connection = MockConnection(id: 0)
        _ = stateMachine.connectionEstablished(connection, maxStreams: 1)

        // Lease
        let request1 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request1)

        // Increase maxStreams with no queued requests
        let action = stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: 4)
        #expect(action == .none())
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func decreaseMaxStreamsBelowCurrentLease() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let refillRequests = stateMachine.refillConnections()
        #expect(refillRequests.count == 1)
        let connection = MockConnection(id: 0)
        _ = stateMachine.connectionEstablished(connection, maxStreams: 4)

        // Lease 2 streams
        let request1 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request1)
        let request2 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request2)

        // Decrease maxStreams — should return .none() and not crash
        let action = stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: 1)
        #expect(action == .none())
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func increaseMaxStreamsButBelowQueuedRequest() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let refillRequests = stateMachine.refillConnections()
        #expect(refillRequests.count == 1)
        let connection = MockConnection(id: 0)
        _ = stateMachine.connectionEstablished(connection, maxStreams: 2)

        // Lease 1 of 2 streams
        let request1 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request1)

        // Queue 5 requests — with 2 maxStreams and 1 used, 1 available so first gets leased immediately
        // Remaining 4 go to queue
        var queuedRequests = [MockRequest<MockConnection>]()
        for _ in 0..<5 {
            let req = MockRequest(connectionType: MockConnection.self)
            queuedRequests.append(req)
            _ = stateMachine.leaseConnection(req)
        }

        // Increase maxStreams from 2 to 6 (increase=4)
        // usedStreams = 2, new capacity = 6-2 = 4 available, but increase is 2, waiting is 4
        let action = stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: 4)
        guard case .leaseConnection(let leasedRequests, let leasedConn) = action.request else {
            Issue.record("Expected leaseConnection action")
            return
        }
        #expect(leasedConn === connection)
        #expect(leasedRequests.count == 2)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func increaseMaxStreamsFewerWaitingThanCapacity() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = nil

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        let refillRequests = stateMachine.refillConnections()
        #expect(refillRequests.count == 1)
        let connection = MockConnection(id: 0)
        _ = stateMachine.connectionEstablished(connection, maxStreams: 1)

        // Lease the only stream
        let request1 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request1)

        // Queue only 1 request
        let request2 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request2)

        // Increase maxStreams from 1 to 10 (increase=9) but only 1 waiting
        let action = stateMachine.connectionReceivedNewMaxStreamSetting(connection.id, newMaxStreamSetting: 10)
        guard case .leaseConnection(let leasedRequests, let leasedConn) = action.request else {
            Issue.record("Expected leaseConnection action")
            return
        }
        #expect(leasedConn === connection)
        // Should only lease 1 (the waiting request count)
        #expect(leasedRequests.count == 1)
    }

    /// Regression test: when the idle-timeout timer fires while a lease request is waiting
    /// in the queue (because a keep-alive is currently consuming the connection's only
    /// stream), `connectionIdleTimerTriggered` reschedules a fresh idle timer. Prior to the
    /// fix, the old idle timer's `TimerCancellationToken` was silently dropped, which in
    /// `ConnectionPool.runTimer` manifests as:
    ///
    ///     SWIFT TASK CONTINUATION MISUSE: runTimer(_:in:) leaked its continuation
    ///     without resuming it.
    ///
    /// because the stored `CheckedContinuation<Void, Never>` is never resumed. This test
    /// drives the state machine into that exact race and asserts the returned action
    /// surfaces the old cancellation token so the caller can resume it.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testIdleTimerReschedulePreservesOldCancellationToken() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 0
        configuration.maximumConnectionSoftLimit = 1
        configuration.maximumConnectionHardLimit = 1
        configuration.keepAliveDuration = .seconds(2)
        configuration.idleTimeoutDuration = .seconds(4)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // Lease a request — triggers creation of a demand connection.
        let request1 = MockRequest(connectionType: MockConnection.self)
        let leaseAction1 = stateMachine.leaseConnection(request1)
        guard case .makeConnection(let makeRequest, _) = leaseAction1.connection else {
            Issue.record("expected .makeConnection, got \(leaseAction1.connection)")
            return
        }

        // Establish the connection — because the request is queued it is leased immediately.
        let connection = MockConnection(id: makeRequest.connectionID)
        let establishedAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        #expect(establishedAction.request == .leaseConnection(.init(element: request1), connection))

        // Release the connection — parks with both keep-alive and idle timers because this
        // is a demand connection (index >= minimumConcurrentConnections).
        let releaseAction = stateMachine.releaseConnection(connection, streams: 1)
        let keepAliveTimer = TestPoolStateMachine.Timer(
            .init(timerID: 0, connectionID: connection.id, usecase: .keepAlive),
            duration: configuration.keepAliveDuration!
        )
        let idleTimer = TestPoolStateMachine.Timer(
            .init(timerID: 1, connectionID: connection.id, usecase: .idleTimeout),
            duration: configuration.idleTimeoutDuration
        )
        #expect(releaseAction.connection == .scheduleTimers([keepAliveTimer, idleTimer]))

        // Register cancellation tokens for both timers (simulates the child task in
        // `ConnectionPool.runTimer` storing its continuation via `timerScheduled`).
        let keepAliveToken = MockTimerCancellationToken(keepAliveTimer)
        let idleToken = MockTimerCancellationToken(idleTimer)
        #expect(stateMachine.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveToken) == nil)
        #expect(stateMachine.timerScheduled(idleTimer, cancelContinuation: idleToken) == nil)

        // Keep-alive fires — the connection transitions to keepAlive: .running, idleTimer: .some.
        let keepAliveFired = stateMachine.timerTriggered(keepAliveTimer)
        #expect(keepAliveFired.connection == .runKeepAlive(connection, keepAliveToken))

        // Queue a second request. Because the running keep-alive consumes the only stream,
        // the request stays in the queue.
        let request2 = MockRequest(connectionType: MockConnection.self)
        _ = stateMachine.leaseConnection(request2)

        // Idle timer fires while the keep-alive is still running and the queue is non-empty.
        // This is the `rescheduleIdleTimer` code path.
        let newIdleTimer = TestPoolStateMachine.Timer(
            .init(timerID: 2, connectionID: connection.id, usecase: .idleTimeout),
            duration: configuration.idleTimeoutDuration
        )
        let idleFired = stateMachine.timerTriggered(idleTimer)

        // The returned action must carry the old idle timer's cancellation token so the
        // pool can resume its continuation. Before the fix this was an empty
        // `.scheduleTimers([newIdleTimer])` and `idleToken` was dropped on the floor.
        #expect(
            idleFired.connection == .makeConnectionsCancelAndScheduleTimers(
                .init(),
                .init(element: idleToken),
                .init(newIdleTimer)
            )
        )
    }

    // MARK: - Keep alive timers

    // Regression test: connectionClosed fires while keepAlive is running on the connection.
    // This can happen during a network partition where the TCP channel closes (firing
    // closeFuture -> onClose -> connectionClosed) before the in-flight keepAlive query
    // failure propagates to connectionKeepAliveFailed. The bug: stats.leasedStreams is
    // decremented by keepAlive.usedStreams (=1), but leasedStreams was never incremented
    // for keepAlive streams — only availableStreams was decremented when keepAlive started.
    // This underflows leasedStreams (UInt16), corrupting pool stats.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("connection closed while keep alive is running", arguments: [true, false]) func connectionClosedWhileKeepAliveRunning(keepAliveReducesAvailableStreams: Bool) {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 2
        configuration.keepAliveDuration = .seconds(10)
        configuration.keepAliveReducesAvailableStreams = keepAliveReducesAvailableStreams

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // 1. Establish the minimum connection (persisted slot, index 0)
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 1)
        let connection0 = MockConnection(id: 0)
        let created0 = stateMachine.connectionEstablished(connection0, maxStreams: 1)
        let keepAliveTimer0 = TestPoolStateMachine.Timer(
            .init(timerID: 0, connectionID: 0, usecase: .keepAlive),
            duration: .seconds(10)
        )
        let cancelToken0 = MockTimerCancellationToken(keepAliveTimer0)
        #expect(created0.connection == .scheduleTimers([keepAliveTimer0]))
        #expect(stateMachine.timerScheduled(keepAliveTimer0, cancelContinuation: cancelToken0) == .none)
        #expect(stateMachine.connections.stats.idle == 1)
        #expect(stateMachine.connections.stats.availableStreams == 1)

        // 2. keepAlive timer fires — connection enters .idle(.running(true))
        //    availableStreams drops from 1 to 0; leasedStreams stays 0
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(connection0.id)
        #expect(keepAliveAction.connection == .runKeepAlive(connection0, cancelToken0))
        #expect(stateMachine.connections.stats.idle == 1)
        #expect(stateMachine.connections.stats.availableStreams == (keepAliveReducesAvailableStreams ? 0 : 1))
        #expect(stateMachine.connections.stats.leasedStreams == (keepAliveReducesAvailableStreams ? 1 : 0), "The keep alive consumes a stream")

        // 3. Network partition: TCP channel drops. NIO fires channelInactive -> closeFuture
        //    completes -> onClose -> connectionClosed BEFORE the keepAlive query failure
        //    propagates to connectionKeepAliveFailed.
        //    Bug: connectionClosed subtracts keepAlive.usedStreams(=1) from leasedStreams(=0),
        //    underflowing leasedStreams to UInt16.max and corrupting pool stats.
        let closedAction = stateMachine.connectionClosed(connection0)
        // Pool should create a new connection to maintain minimum
        #expect(closedAction.connection == .makeConnection(.init(connectionID: 1), []))

        // After the fix: stats must be consistent — no underflow
        #expect(stateMachine.connections.stats.leasedStreams == 0)
        #expect(stateMachine.connections.stats.availableStreams == 0)
        #expect(stateMachine.connections.stats.idle == 0)
        #expect(stateMachine.connections.stats.runningKeepAlive == 0)
    }

    // Regression test: network partition closes multiple minimum connections simultaneously.
    // A second connectionClosed call (for a connection that was still idle when swapForDeletion
    // runs) must not crash even when pool is in connectionCreationFailing state.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func networkPartitionAllMinimumConnectionsClose() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 2
        configuration.maximumConnectionSoftLimit = 2
        configuration.maximumConnectionHardLimit = 4
        configuration.keepAliveDuration = .seconds(10)

        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self,
            clock: MockClock()
        )

        // 1. Establish both minimum connections
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 2)
        let connection0 = MockConnection(id: 0)
        let connection1 = MockConnection(id: 1)

        let created0 = stateMachine.connectionEstablished(connection0, maxStreams: 1)
        let timer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(10))
        let cancel0 = MockTimerCancellationToken(timer0)
        #expect(stateMachine.timerScheduled(timer0, cancelContinuation: cancel0) == .none)
        _ = created0

        let created1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        let timer1 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(10))
        let cancel1 = MockTimerCancellationToken(timer1)
        #expect(stateMachine.timerScheduled(timer1, cancelContinuation: cancel1) == .none)
        _ = created1
        #expect(stateMachine.connections.stats.idle == 2)

        // 2. Network partition: connection1 drops first
        let closed1 = stateMachine.connectionClosed(connection1)
        // Pool makes a new connection to maintain minimum; cancel connection1's keepAlive timer
        #expect(closed1.connection == .makeConnection(.init(connectionID: 2), [cancel1]))

        // 3. Replacement (id=2) fails — pool enters connectionCreationFailing
        let failAction = stateMachine.connectionEstablishFailed(SomeError(), for: .init(connectionID: 2))
        #expect(failAction.request == .none)

        // 4. 8 seconds later (as in the real crash): connection0 also drops
        //    Pool is in connectionCreationFailing; connection0 is still idle in the array.
        let closed0 = stateMachine.connectionClosed(connection0)
        // Pool should try to create another connection; cancel connection0's keepAlive timer
        guard case .makeConnection(let newRequest, _) = closed0.connection else {
            Issue.record("Expected .makeConnection, got \(closed0.connection)")
            return
        }
        #expect(newRequest.connectionID != 0)
        #expect(newRequest.connectionID != 1)

        // Stats must be consistent after both closes
        #expect(stateMachine.connections.stats.idle == 0)
        #expect(stateMachine.connections.stats.leasedStreams == 0)
    }
}

struct SomeError: Error {}
