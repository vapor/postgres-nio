import XCTest
@testable import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest,
    MockRequest.ID,
    MockTimerCancellationToken
>

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PoolStateMachineTests: XCTestCase {

    func testConnectionsAreCreatedAndParkedOnStartup() {
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
            XCTAssertEqual(requests.count, 2)
            let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
            let connection1KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(10))
            let connection1KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer)
            XCTAssertEqual(createdAction1.request, .none)
            XCTAssertEqual(createdAction1.connection, .scheduleTimers([connection1KeepAliveTimer]))

            XCTAssertEqual(stateMachine.timerScheduled(connection1KeepAliveTimer, cancelContinuation: connection1KeepAliveTimerCancellationToken), .none)

            let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
            let connection2KeepAliveTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(10))
            let connection2KeepAliveTimerCancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer)
            XCTAssertEqual(createdAction2.request, .none)
            XCTAssertEqual(createdAction2.connection, .scheduleTimers([connection2KeepAliveTimer]))
            XCTAssertEqual(stateMachine.timerScheduled(connection2KeepAliveTimer, cancelContinuation: connection2KeepAliveTimerCancellationToken), .none)
        }
    }

    func testConnectionsNoKeepAliveRun() {
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
        XCTAssertEqual(requests.count, 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .none)
        XCTAssertEqual(createdAction1.connection, .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest1.request, .leaseConnection(.init(element: request1), connection1))

        // release connection 1
        XCTAssertEqual(stateMachine.releaseConnection(connection1, streams: 1), .none())

        // lease connection 1
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest2.request, .leaseConnection(.init(element: request2), connection1))

        // request connection while none is available
        let request3 = MockRequest()
        let leaseRequest3 = stateMachine.leaseConnection(request3)
        XCTAssertEqual(leaseRequest3.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest3.request, .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request3), connection2))
        XCTAssertEqual(createdAction2.connection, .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        XCTAssertEqual(
            stateMachine.releaseConnection(connection2, streams: 1),
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        XCTAssertEqual(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken), .none)
        XCTAssertEqual(stateMachine.timerTriggered(connection2IdleTimer), .init(request: .none, connection: .closeConnection(connection2, [connection2IdleTimerCancellationToken])))
    }

    func testOnlyOverflowConnections() {
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
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)

        // request connection while none is available
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest1.request, .leaseConnection(.init(element: request2), connection1))
        XCTAssertEqual(releaseRequest1.connection, .none)

        // connection 2 comes up and should be closed right away
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .none)
        XCTAssertEqual(createdAction2.connection, .closeConnection(connection2, []))
        XCTAssertEqual(stateMachine.connectionClosed(connection2), .none())

        // release connection 1 should be closed as well
        let releaseRequest2 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest2.request, .none)
        XCTAssertEqual(releaseRequest2.connection, .closeConnection(connection1, []))

        let shutdownAction = stateMachine.triggerForceShutdown()
        XCTAssertEqual(shutdownAction.request, .failRequests(.init(), .poolShutdown))
        XCTAssertEqual(shutdownAction.connection, .shutdown(.init()))
    }

    func testDemandConnectionIsMadePermanentIfPermanentIsClose() {
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
        XCTAssertEqual(requests.count, 1)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .none)
        XCTAssertEqual(createdAction1.connection, .scheduleTimers([]))

        // lease connection 1
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .cancelTimers([]))
        XCTAssertEqual(leaseRequest1.request, .leaseConnection(.init(element: request1), connection1))

        // request connection while none is available
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // make connection 2 and lease immediately
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request2), connection2))
        XCTAssertEqual(createdAction2.connection, .none)

        // release connection 2
        let connection2IdleTimer = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .idleTimeout), duration: configuration.idleTimeoutDuration)
        let connection2IdleTimerCancellationToken = MockTimerCancellationToken(connection2IdleTimer)
        XCTAssertEqual(
            stateMachine.releaseConnection(connection2, streams: 1),
            .init(request: .none, connection: .scheduleTimers([connection2IdleTimer]))
        )

        XCTAssertEqual(stateMachine.timerScheduled(connection2IdleTimer, cancelContinuation: connection2IdleTimerCancellationToken), .none)

        // connection 1 is dropped
        XCTAssertEqual(stateMachine.connectionClosed(connection1), .init(request: .none, connection: .cancelTimers([connection2IdleTimerCancellationToken])))
    }

    func testReleaseLoosesRaceAgainstClosed() {
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
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1 and lease immediately
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)

        // connection got closed
        let closedAction = stateMachine.connectionClosed(connection1)
        XCTAssertEqual(closedAction.connection, .none)
        XCTAssertEqual(closedAction.request, .none)

        // release connection 1 should be leased again immediately
        let releaseRequest1 = stateMachine.releaseConnection(connection1, streams: 1)
        XCTAssertEqual(releaseRequest1.request, .none)
        XCTAssertEqual(releaseRequest1.connection, .none)
    }

    func testKeepAliveOnClosingConnection() {
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
        XCTAssertEqual(requests.count, 0)

        // request connection while none exists
        let request1 = MockRequest()
        let leaseRequest1 = stateMachine.leaseConnection(request1)
        XCTAssertEqual(leaseRequest1.connection, .makeConnection(.init(connectionID: 0), []))
        XCTAssertEqual(leaseRequest1.request, .none)

        // make connection 1
        let connection1 = MockConnection(id: 0)
        let createdAction1 = stateMachine.connectionEstablished(connection1, maxStreams: 1)
        XCTAssertEqual(createdAction1.request, .leaseConnection(.init(element: request1), connection1))
        XCTAssertEqual(createdAction1.connection, .none)
        _ = stateMachine.releaseConnection(connection1, streams: 1)

        // trigger keep alive
        let keepAliveAction1 = stateMachine.connectionKeepAliveTimerTriggered(connection1.id)
        XCTAssertEqual(keepAliveAction1.connection, .runKeepAlive(connection1, nil))

        // fail keep alive and cause closed
        let keepAliveFailed1 = stateMachine.connectionKeepAliveFailed(connection1.id)
        XCTAssertEqual(keepAliveFailed1.connection, .closeConnection(connection1, []))
        connection1.closeIfClosing()

        // request connection while none exists anymore
        let request2 = MockRequest()
        let leaseRequest2 = stateMachine.leaseConnection(request2)
        XCTAssertEqual(leaseRequest2.connection, .makeConnection(.init(connectionID: 1), []))
        XCTAssertEqual(leaseRequest2.request, .none)

        // make connection 2
        let connection2 = MockConnection(id: 1)
        let createdAction2 = stateMachine.connectionEstablished(connection2, maxStreams: 1)
        XCTAssertEqual(createdAction2.request, .leaseConnection(.init(element: request2), connection2))
        XCTAssertEqual(createdAction2.connection, .none)
        _ = stateMachine.releaseConnection(connection2, streams: 1)

        // trigger keep alive while connection is still open
        let keepAliveAction2 = stateMachine.connectionKeepAliveTimerTriggered(connection2.id)
        XCTAssertEqual(keepAliveAction2.connection, .runKeepAlive(connection2, nil))

        // close connection in the middle of keep alive
        connection2.close()
        connection2.closeIfClosing()

        // fail keep alive and cause closed
        let keepAliveFailed2 = stateMachine.connectionKeepAliveFailed(connection2.id)
        XCTAssertEqual(keepAliveFailed2.connection, .closeConnection(connection2, []))
    }

    func testConnectionIsEstablishedAfterFailedKeepAliveIfNotEnoughConnectionsLeft() {
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
        XCTAssertEqual(requests.count, 1)

        // one connection should exist
        let request = MockRequest()
        let leaseRequest = stateMachine.leaseConnection(request)
        XCTAssertEqual(leaseRequest.connection, .none)
        XCTAssertEqual(leaseRequest.request, .none)

        // make connection 1
        let connection = MockConnection(id: 0)
        let createdAction = stateMachine.connectionEstablished(connection, maxStreams: 1)
        XCTAssertEqual(createdAction.request, .leaseConnection(.init(element: request), connection))
        XCTAssertEqual(createdAction.connection, .none)
        _ = stateMachine.releaseConnection(connection, streams: 1)

        // trigger keep alive
        let keepAliveAction = stateMachine.connectionKeepAliveTimerTriggered(connection.id)
        XCTAssertEqual(keepAliveAction.connection, .runKeepAlive(connection, nil))

        // fail keep alive, cause closed and make new connection
        let keepAliveFailed = stateMachine.connectionKeepAliveFailed(connection.id)
        XCTAssertEqual(keepAliveFailed.connection, .closeConnection(connection, []))
        let connectionClosed = stateMachine.connectionClosed(connection)
        XCTAssertEqual(connectionClosed.connection, .makeConnection(.init(connectionID: 1), []))
        connection.closeIfClosing()
    }

    func testConnectionsRequestedInBurstsWork() {
        var configuration = PoolConfiguration()
        configuration.minimumConnectionCount = 1
        configuration.maximumConnectionSoftLimit = 20
        configuration.maximumConnectionHardLimit = 20
        configuration.keepAliveDuration = .seconds(30)
        configuration.idleTimeoutDuration = .seconds(60)


        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        func makeKeepAliveTimer(timerID: Int, connectionID: Int) -> (timer: TestPoolStateMachine.Timer, cancellationToken: MockTimerCancellationToken) {
            let timer = TestPoolStateMachine.Timer(.init(timerID: timerID, connectionID: connectionID, usecase: .keepAlive), duration: .seconds(30))
            return (timer, MockTimerCancellationToken(timer))
        }
        func makeIdleTimer(timerID: Int, connectionID: Int) -> (timer: TestPoolStateMachine.Timer, cancellationToken: MockTimerCancellationToken) {
            let timer = TestPoolStateMachine.Timer(.init(timerID: timerID, connectionID: connectionID, usecase: .idleTimeout), duration: .seconds(60))
            return (timer, MockTimerCancellationToken(timer))
        }

        // Connections
        let conn8 = MockConnection(id: 8)
        let conn14 = MockConnection(id: 14)
        let conn11 = MockConnection(id: 11)
        let conn3 = MockConnection(id: 3)
        let conn16 = MockConnection(id: 16)
        let conn13 = MockConnection(id: 13)
        let conn10 = MockConnection(id: 10)
        let conn7 = MockConnection(id: 7)
        let conn5 = MockConnection(id: 5)
        let conn18 = MockConnection(id: 18)
        let conn9 = MockConnection(id: 9)
        let conn0 = MockConnection(id: 0)
        let conn4 = MockConnection(id: 4)
        let conn19 = MockConnection(id: 19)
        let conn6 = MockConnection(id: 6)
        let conn12 = MockConnection(id: 12)
        let conn17 = MockConnection(id: 17)
        let conn15 = MockConnection(id: 15)
        let conn2 = MockConnection(id: 2)
        let conn1 = MockConnection(id: 1)

        stateMachine.refillConnections()
        for _ in 0..<20 { // request 20 connections
            stateMachine.leaseConnection(.init())
        }

        stateMachine.connectionEstablished(conn8, maxStreams: 1)
        stateMachine.connectionEstablished(conn14, maxStreams: 1)
        stateMachine.connectionEstablished(conn11, maxStreams: 1)
        stateMachine.connectionEstablished(conn3, maxStreams: 1)
        stateMachine.releaseConnection(conn8, streams: 1)
        stateMachine.releaseConnection(conn14, streams: 1)
        Optional("hello")
        Optional("hello")
        stateMachine.releaseConnection(conn8, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn14, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn11, streams: 1)
        stateMachine.releaseConnection(conn8, streams: 1)
        stateMachine.releaseConnection(conn14, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn3, streams: 1)
        Optional("hello")
        Optional("hello")
        Optional("hello")
        stateMachine.releaseConnection(conn11, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn14, streams: 1)
        stateMachine.releaseConnection(conn8, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn3, streams: 1)
        Optional("hello")
        Optional("hello")
        stateMachine.releaseConnection(conn11, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn8, streams: 1)
        Optional("hello")
        stateMachine.releaseConnection(conn11, streams: 1)
        stateMachine.releaseConnection(conn14, streams: 1)
        Optional("hello")
        Optional("hello")
        stateMachine.releaseConnection(conn8, streams: 1)
        stateMachine.releaseConnection(conn3, streams: 1)
        Optional("hello")
        Optional("hello")
        let conn8KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 8)
        stateMachine.timerScheduled(conn8KeepAliveTimer.timer, cancelContinuation: conn8KeepAliveTimer.cancellationToken)
        let conn8IdleTimer = makeIdleTimer(timerID: 1, connectionID: 8)
        stateMachine.timerScheduled(conn8IdleTimer.timer, cancelContinuation: conn8IdleTimer.cancellationToken)
        stateMachine.releaseConnection(conn14, streams: 1)
        stateMachine.releaseConnection(conn11, streams: 1)
        Optional("hello")
        let conn3IdleTimer = makeIdleTimer(timerID: 1, connectionID: 3)
        stateMachine.timerScheduled(conn3IdleTimer.timer, cancelContinuation: conn3IdleTimer.cancellationToken)
        Optional("hello")
        let conn3KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 3)
        stateMachine.timerScheduled(conn3KeepAliveTimer.timer, cancelContinuation: conn3KeepAliveTimer.cancellationToken)
        stateMachine.connectionEstablished(conn16, maxStreams: 1)
        stateMachine.connectionEstablished(conn13, maxStreams: 1)
        stateMachine.connectionEstablished(conn10, maxStreams: 1)
        stateMachine.connectionEstablished(conn7, maxStreams: 1)
        let conn14IdleTimer = makeIdleTimer(timerID: 1, connectionID: 14)
        stateMachine.timerScheduled(conn14IdleTimer.timer, cancelContinuation: conn14IdleTimer.cancellationToken)
        let conn14KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 14)
        stateMachine.timerScheduled(conn14KeepAliveTimer.timer, cancelContinuation: conn14KeepAliveTimer.cancellationToken)
        let conn11IdleTimer = makeIdleTimer(timerID: 1, connectionID: 11)
        stateMachine.timerScheduled(conn11IdleTimer.timer, cancelContinuation: conn11IdleTimer.cancellationToken)
        let conn11KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 11)
        stateMachine.timerScheduled(conn11KeepAliveTimer.timer, cancelContinuation: conn11KeepAliveTimer.cancellationToken)
        let conn16KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 16)
        stateMachine.timerScheduled(conn16KeepAliveTimer.timer, cancelContinuation: conn16KeepAliveTimer.cancellationToken)
        let conn16IdleTimer = makeIdleTimer(timerID: 1, connectionID: 16)
        stateMachine.timerScheduled(conn16IdleTimer.timer, cancelContinuation: conn16IdleTimer.cancellationToken)
        let conn13KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 13)
        stateMachine.timerScheduled(conn13KeepAliveTimer.timer, cancelContinuation: conn13KeepAliveTimer.cancellationToken)
        let conn13IdleTimer = makeIdleTimer(timerID: 1, connectionID: 13)
        stateMachine.timerScheduled(conn13IdleTimer.timer, cancelContinuation: conn13IdleTimer.cancellationToken)
        let conn10KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 10)
        stateMachine.timerScheduled(conn10KeepAliveTimer.timer, cancelContinuation: conn10KeepAliveTimer.cancellationToken)
        let conn10IdleTimer = makeIdleTimer(timerID: 1, connectionID: 10)
        stateMachine.timerScheduled(conn10IdleTimer.timer, cancelContinuation: conn10IdleTimer.cancellationToken)
        let conn7KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 7)
        stateMachine.timerScheduled(conn7KeepAliveTimer.timer, cancelContinuation: conn7KeepAliveTimer.cancellationToken)
        let conn7IdleTimer = makeIdleTimer(timerID: 1, connectionID: 7)
        stateMachine.timerScheduled(conn7IdleTimer.timer, cancelContinuation: conn7IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn5, maxStreams: 1)
        let conn5KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 5)
        stateMachine.timerScheduled(conn5KeepAliveTimer.timer, cancelContinuation: conn5KeepAliveTimer.cancellationToken)
        let conn5IdleTimer = makeIdleTimer(timerID: 1, connectionID: 5)
        stateMachine.timerScheduled(conn5IdleTimer.timer, cancelContinuation: conn5IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn18, maxStreams: 1)
        let conn18KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 18)
        stateMachine.timerScheduled(conn18KeepAliveTimer.timer, cancelContinuation: conn18KeepAliveTimer.cancellationToken)
        let conn18IdleTimer = makeIdleTimer(timerID: 1, connectionID: 18)
        stateMachine.timerScheduled(conn18IdleTimer.timer, cancelContinuation: conn18IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn9, maxStreams: 1)
        let conn9IdleTimer = makeIdleTimer(timerID: 1, connectionID: 9)
        stateMachine.timerScheduled(conn9IdleTimer.timer, cancelContinuation: conn9IdleTimer.cancellationToken)
        let conn9KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 9)
        stateMachine.timerScheduled(conn9KeepAliveTimer.timer, cancelContinuation: conn9KeepAliveTimer.cancellationToken)
        stateMachine.connectionEstablished(conn0, maxStreams: 1)
        let conn0KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 0)
        stateMachine.timerScheduled(conn0KeepAliveTimer.timer, cancelContinuation: conn0KeepAliveTimer.cancellationToken)
        let conn0IdleTimer = makeIdleTimer(timerID: 1, connectionID: 0)
        stateMachine.timerScheduled(conn0IdleTimer.timer, cancelContinuation: conn0IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn4, maxStreams: 1)
        stateMachine.connectionEstablished(conn19, maxStreams: 1)
        let conn4KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 4)
        stateMachine.timerScheduled(conn4KeepAliveTimer.timer, cancelContinuation: conn4KeepAliveTimer.cancellationToken)
        let conn19IdleTimer = makeIdleTimer(timerID: 1, connectionID: 19)
        stateMachine.timerScheduled(conn19IdleTimer.timer, cancelContinuation: conn19IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn6, maxStreams: 1)
        let conn4IdleTimer = makeIdleTimer(timerID: 1, connectionID: 4)
        stateMachine.timerScheduled(conn4IdleTimer.timer, cancelContinuation: conn4IdleTimer.cancellationToken)
        let conn19KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 19)
        stateMachine.timerScheduled(conn19KeepAliveTimer.timer, cancelContinuation: conn19KeepAliveTimer.cancellationToken)
        let conn6KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 6)
        stateMachine.timerScheduled(conn6KeepAliveTimer.timer, cancelContinuation: conn6KeepAliveTimer.cancellationToken)
        stateMachine.connectionEstablished(conn12, maxStreams: 1)
        let conn6IdleTimer = makeIdleTimer(timerID: 1, connectionID: 6)
        stateMachine.timerScheduled(conn6IdleTimer.timer, cancelContinuation: conn6IdleTimer.cancellationToken)
        stateMachine.connectionEstablished(conn17, maxStreams: 1)
        stateMachine.connectionEstablished(conn15, maxStreams: 1)
        let conn12KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 12)
        stateMachine.timerScheduled(conn12KeepAliveTimer.timer, cancelContinuation: conn12KeepAliveTimer.cancellationToken)
        stateMachine.connectionEstablished(conn2, maxStreams: 1)
        stateMachine.connectionEstablished(conn1, maxStreams: 1)
        let conn12IdleTimer = makeIdleTimer(timerID: 1, connectionID: 12)
        stateMachine.timerScheduled(conn12IdleTimer.timer, cancelContinuation: conn12IdleTimer.cancellationToken)
        let conn17KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 17)
        stateMachine.timerScheduled(conn17KeepAliveTimer.timer, cancelContinuation: conn17KeepAliveTimer.cancellationToken)
        let conn17IdleTimer = makeIdleTimer(timerID: 1, connectionID: 17)
        stateMachine.timerScheduled(conn17IdleTimer.timer, cancelContinuation: conn17IdleTimer.cancellationToken)
        let conn15KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 15)
        stateMachine.timerScheduled(conn15KeepAliveTimer.timer, cancelContinuation: conn15KeepAliveTimer.cancellationToken)
        let conn15IdleTimer = makeIdleTimer(timerID: 1, connectionID: 15)
        stateMachine.timerScheduled(conn15IdleTimer.timer, cancelContinuation: conn15IdleTimer.cancellationToken)
        let conn2KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 2)
        stateMachine.timerScheduled(conn2KeepAliveTimer.timer, cancelContinuation: conn2KeepAliveTimer.cancellationToken)
        let conn2IdleTimer = makeIdleTimer(timerID: 1, connectionID: 2)
        stateMachine.timerScheduled(conn2IdleTimer.timer, cancelContinuation: conn2IdleTimer.cancellationToken)
        let conn1KeepAliveTimer = makeKeepAliveTimer(timerID: 0, connectionID: 1)
        stateMachine.timerScheduled(conn1KeepAliveTimer.timer, cancelContinuation: conn1KeepAliveTimer.cancellationToken)
        let conn1IdleTimer = makeIdleTimer(timerID: 0, connectionID: 1)
        stateMachine.timerScheduled(conn1IdleTimer.timer, cancelContinuation: conn1IdleTimer.cancellationToken)

        stateMachine.timerTriggered(conn14KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(14)
        stateMachine.timerTriggered(conn8KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(8)
        stateMachine.timerTriggered(conn0KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(0)
        stateMachine.timerTriggered(conn12KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(12)
        stateMachine.timerTriggered(conn4KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(4)
        stateMachine.timerTriggered(conn6KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(6)
        stateMachine.timerTriggered(conn5KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(5)
        stateMachine.timerTriggered(conn10KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(10)
        stateMachine.timerTriggered(conn18KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(18)
        stateMachine.timerTriggered(conn15KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(15)
        stateMachine.timerTriggered(conn1KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(1)
        stateMachine.timerTriggered(conn3KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(3)
        stateMachine.timerTriggered(conn16KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(16)
        stateMachine.timerTriggered(conn7KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(7)
        stateMachine.timerTriggered(conn11KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(11)
        stateMachine.timerTriggered(conn19KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(19)
        stateMachine.timerTriggered(conn17KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(17)
        stateMachine.timerTriggered(conn2KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(2)
        stateMachine.timerTriggered(conn9KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(9)
        stateMachine.timerTriggered(conn13KeepAliveTimer.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(13)

        stateMachine.connectionKeepAliveDone(conn14)
        stateMachine.connectionKeepAliveDone(conn8)
        stateMachine.connectionKeepAliveDone(conn0)
        stateMachine.connectionKeepAliveDone(conn10)
        let conn14KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 14)
        stateMachine.timerScheduled(conn14KeepAliveTimer2.timer, cancelContinuation: conn14KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn12)
        let conn0KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 0)
        stateMachine.timerScheduled(conn0KeepAliveTimer2.timer, cancelContinuation: conn0KeepAliveTimer2.cancellationToken)
        let conn10KeepAliveTimer2 = makeKeepAliveTimer(timerID: 1, connectionID: 10)
        stateMachine.timerScheduled(conn10KeepAliveTimer2.timer, cancelContinuation: conn10KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn3)
        let conn8KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 8)
        stateMachine.timerScheduled(conn8KeepAliveTimer2.timer, cancelContinuation: conn8KeepAliveTimer2.cancellationToken)
        let conn12KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 12)
        stateMachine.timerScheduled(conn12KeepAliveTimer2.timer, cancelContinuation: conn12KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn6)
        let conn3KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 3)
        stateMachine.timerScheduled(conn3KeepAliveTimer2.timer, cancelContinuation: conn3KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn15)
        stateMachine.connectionKeepAliveDone(conn16)
        stateMachine.connectionKeepAliveDone(conn9)
        stateMachine.connectionKeepAliveDone(conn18)
        let conn15KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 15)
        stateMachine.timerScheduled(conn15KeepAliveTimer2.timer, cancelContinuation: conn15KeepAliveTimer2.cancellationToken)
        let conn6KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 6)
        stateMachine.timerScheduled(conn6KeepAliveTimer2.timer, cancelContinuation: conn6KeepAliveTimer2.cancellationToken)
        let conn16KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 16)
        stateMachine.timerScheduled(conn16KeepAliveTimer2.timer, cancelContinuation: conn16KeepAliveTimer2.cancellationToken)
        let conn9KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 9)
        stateMachine.timerScheduled(conn9KeepAliveTimer2.timer, cancelContinuation: conn9KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn1)
        let conn18KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 18)
        stateMachine.timerScheduled(conn18KeepAliveTimer2.timer, cancelContinuation: conn18KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn7)
        stateMachine.connectionKeepAliveDone(conn13)
        stateMachine.connectionKeepAliveDone(conn17)
        stateMachine.connectionKeepAliveDone(conn19)
        let conn1KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 1)
        stateMachine.timerScheduled(conn1KeepAliveTimer2.timer, cancelContinuation: conn1KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn2)
        let conn7KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 7)
        stateMachine.timerScheduled(conn7KeepAliveTimer2.timer, cancelContinuation: conn7KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn5)
        let conn13KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 13)
        stateMachine.timerScheduled(conn13KeepAliveTimer2.timer, cancelContinuation: conn13KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn11)
        let conn17KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 17)
        stateMachine.timerScheduled(conn17KeepAliveTimer2.timer, cancelContinuation: conn17KeepAliveTimer2.cancellationToken)
        stateMachine.connectionKeepAliveDone(conn4)
        let conn19KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 19)
        stateMachine.timerScheduled(conn19KeepAliveTimer2.timer, cancelContinuation: conn19KeepAliveTimer2.cancellationToken)
        let conn2KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 2)
        stateMachine.timerScheduled(conn2KeepAliveTimer2.timer, cancelContinuation: conn2KeepAliveTimer2.cancellationToken)
        let conn5KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 5)
        stateMachine.timerScheduled(conn5KeepAliveTimer2.timer, cancelContinuation: conn5KeepAliveTimer2.cancellationToken)
        let conn11KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 11)
        stateMachine.timerScheduled(conn11KeepAliveTimer2.timer, cancelContinuation: conn11KeepAliveTimer2.cancellationToken)
        let conn4KeepAliveTimer2 = makeKeepAliveTimer(timerID: 2, connectionID: 4)
        stateMachine.timerScheduled(conn4KeepAliveTimer2.timer, cancelContinuation: conn4KeepAliveTimer2.cancellationToken)
        stateMachine.timerTriggered(conn3IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(3)
        stateMachine.timerTriggered(conn10IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(10)
        stateMachine.timerTriggered(conn7IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(7)
        stateMachine.timerTriggered(conn13IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(13)
        stateMachine.timerTriggered(conn18IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(18)
        stateMachine.timerTriggered(conn5IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(5)
        stateMachine.timerTriggered(conn9IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(9)
        stateMachine.timerTriggered(conn0IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(0)
        stateMachine.timerTriggered(conn4IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(4)
        stateMachine.timerTriggered(conn19IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(19)
        stateMachine.timerTriggered(conn6IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(6)
        stateMachine.timerTriggered(conn12IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(12)
        stateMachine.timerTriggered(conn17IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(17)
        stateMachine.timerTriggered(conn2IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(2)
        stateMachine.timerTriggered(conn15IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(15)
        stateMachine.timerTriggered(conn1IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(1)
        stateMachine.timerTriggered(conn14KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(14)
        stateMachine.timerTriggered(conn8KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(8)
        stateMachine.timerTriggered(conn14IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(14)
        stateMachine.timerTriggered(conn16KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(16)
        stateMachine.timerTriggered(conn18KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(18)
        stateMachine.timerTriggered(conn9KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(9)
        stateMachine.timerTriggered(conn8IdleTimer.timer)
//        stateMachine.connectionIdleTimerTriggered(8)
        stateMachine.timerTriggered(conn10KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(10)
        stateMachine.timerTriggered(conn17KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(17)
        stateMachine.timerTriggered(conn19KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(19)
        stateMachine.timerTriggered(conn6KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(6)
        stateMachine.timerTriggered(conn2KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(2)
        stateMachine.timerTriggered(conn5KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(5)
        stateMachine.timerTriggered(conn11KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(11)
        stateMachine.timerTriggered(conn1KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(1)
        // Burst done: 1/50
        stateMachine.timerTriggered(conn0KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(0)
        stateMachine.timerTriggered(conn7KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(7)
        stateMachine.timerTriggered(conn3KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(3)
        stateMachine.leaseConnection(MockRequest())
        stateMachine.connectionClosed(conn5)
        stateMachine.connectionClosed(conn6)
        stateMachine.connectionClosed(conn3)
        stateMachine.connectionClosed(conn19)
        stateMachine.connectionClosed(conn10)
        stateMachine.connectionClosed(conn12)
        stateMachine.connectionClosed(conn17)
        stateMachine.connectionClosed(conn18)
        stateMachine.connectionClosed(conn9)
        stateMachine.connectionClosed(conn1)
        stateMachine.timerTriggered(conn13KeepAliveTimer2.timer)
//        stateMachine.connectionKeepAliveTimerTriggered(13)
    }

}
