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
        print(failedAction3)
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

        let connections = (0..<5).map { MockConnection(id: $0) }
        let connectedActions = (0..<5).map { stateMachine.connectionEstablished(connections[$0], maxStreams: 1) }
        #expect(connectedActions[0].connection == .makeConnectionsCancelAndScheduleTimers(.init(element: .init(connectionID: 3)), [], []))
        #expect(connectedActions[1].connection == .makeConnectionsCancelAndScheduleTimers(.init(element: .init(connectionID: 4)), [], []))
        #expect(connectedActions[2].connection == .cancelTimers([]))
        #expect(connectedActions[3].connection == .cancelTimers([]))
        #expect(connectedActions[4].connection == .cancelTimers([]))
        for i in 0..<5 {
            #expect(connectedActions[i].request == .leaseConnection([requests[i]], connections[i]))
        }
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
}
