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
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testBackingOffRequests() {
        struct ConnectionFailed: Error, Equatable {}
        let clock = MockClock()
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
            clock: clock
        )

        // refill pool
        let requests = stateMachine.refillConnections()
        #expect(requests.count == 2)

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

        let request = MockRequest(connectionType: MockConnection.self)
        let leaseAction = stateMachine.leaseConnection(request)
        #expect(leaseAction.request == .none)
        #expect(leaseAction.connection == .none)

        clock.advance(to: clock.now.advanced(by: .seconds(30)))

        // fail connection 2. Connection request is removed as we already have a failing connection
        let failedAction2 = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[1])
        #expect(failedAction2.request == .none)
        #expect(failedAction2.connection == .cancelTimers(.init()))
        #expect(stateMachine.connections.connections.count == 1)

        let backOffDone = stateMachine.connectionCreationBackoffDone(requests[0].connectionID)
        #expect(backOffDone.request == .none)
        #expect(backOffDone.connection == .makeConnection(requests[0], []))

        // fail connection 1 again
        let failedAction3 = stateMachine.connectionEstablishFailed(ConnectionFailed(), for: requests[0])
        #expect(failedAction3.request == .failRequests([request], ConnectionPoolError.connectionCreationCircuitBreakerTripped))
        switch failedAction3.connection {
        case .scheduleTimers(let timers):
            #expect(timers.count == 1)
            #expect(timers.first?.underlying.usecase == .backoff)
        default:
            Issue.record()
        }

        // lease fails immediately as we are in circuitBreak state
        let request2 = MockRequest(connectionType: MockConnection.self)
        let leaseAction2 = stateMachine.leaseConnection(request2)
        #expect(leaseAction2.request == .failRequest(request2, ConnectionPoolError.connectionCreationCircuitBreakerTripped))
        #expect(leaseAction2.connection == .none)

        let backOffDone2 = stateMachine.connectionCreationBackoffDone(requests[0].connectionID)
        #expect(backOffDone2.request == .none)
        #expect(backOffDone2.connection == .makeConnection(requests[0], []))

        // make connection
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        let connection2KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 0, usecase: .keepAlive), duration: .seconds(2))
        #expect(createdAction.request == .none)
        #expect(createdAction.connection == .scheduleTimers([connection2KeepAliveTimer]))

        // lease connection (successful)
        let request3 = MockRequest(connectionType: MockConnection.self)
        let leaseAction3 = stateMachine.leaseConnection(request3)
        #expect(leaseAction3.request == .leaseConnection(.init(element: request3), connection))
        #expect(leaseAction3.connection == .none)

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
    }
}
