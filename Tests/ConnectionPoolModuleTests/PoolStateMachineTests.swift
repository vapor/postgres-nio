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
    MockTimerCancellationToken
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
        #expect(shutdownAction.connection == .shutdown(.init()))
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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
            timerCancellationTokenType: MockTimerCancellationToken.self
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

}
