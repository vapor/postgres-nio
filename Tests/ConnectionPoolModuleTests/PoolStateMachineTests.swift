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
        configuration.maximumConnectionSoftLimit = 3
        configuration.maximumConnectionHardLimit = 3
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
        let connection0 = MockConnection(id: 0)
        let connection2 = MockConnection(id: 2)
        let connection1 = MockConnection(id: 1)

        let request0 = MockRequest()
        let request1 = MockRequest()
        let request2 = MockRequest()

        stateMachine.refillConnections()

        stateMachine.leaseConnection(request1)
        stateMachine.leaseConnection(request0)
        stateMachine.leaseConnection(request2)

        stateMachine.connectionEstablished(connection2, maxStreams: 1)
        stateMachine.connectionEstablished(connection1, maxStreams: 1)
        // do work...
        // Optional("hello")
        // Optional("hello")
        stateMachine.releaseConnection(connection2, streams: 1)
        stateMachine.releaseConnection(connection1, streams: 1)

        let connection1KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 1, usecase: .keepAlive), duration: .seconds(30))
        let connection1KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer0)
        _ = stateMachine.timerScheduled(connection1KeepAliveTimer0, cancelContinuation: connection1KeepAliveTimer0CancellationToken)
        let connection1IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 1, usecase: .idleTimeout), duration: .seconds(60))
        let connection1IdleTimer1CancellationToken = MockTimerCancellationToken(connection1IdleTimer1)
        _ = stateMachine.timerScheduled(connection1IdleTimer1, cancelContinuation: connection1IdleTimer1CancellationToken)

        // do more work...
        // Optional("hello")
        stateMachine.releaseConnection(connection2, streams: 1)
        
        let connection2KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 2, usecase: .keepAlive), duration: .seconds(30))
        let connection2KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer0)
        _ = stateMachine.timerScheduled(connection2KeepAliveTimer0, cancelContinuation: connection2KeepAliveTimer0CancellationToken)
        let connection2IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 2, usecase: .idleTimeout), duration: .seconds(60))
        let connection2IdleTimer1CancellationToken = MockTimerCancellationToken(connection2IdleTimer1)
        _ = stateMachine.timerScheduled(connection2IdleTimer1, cancelContinuation: connection2IdleTimer1CancellationToken)

        stateMachine.connectionEstablished(connection0, maxStreams: 1)
        
        let connection0IdleTimer1 = TestPoolStateMachine.Timer(.init(timerID: 1, connectionID: 0, usecase: .idleTimeout), duration: .seconds(60))
        let connection0IdleTimer1CancellationToken = MockTimerCancellationToken(connection0IdleTimer1)
        _ = stateMachine.timerScheduled(connection0IdleTimer1, cancelContinuation: connection0IdleTimer1CancellationToken)
        let connection0KeepAliveTimer0 = TestPoolStateMachine.Timer(.init(timerID: 0, connectionID: 0, usecase: .keepAlive), duration: .seconds(30))
        let connection0KeepAliveTimer0CancellationToken = MockTimerCancellationToken(connection0KeepAliveTimer0)
        _ = stateMachine.timerScheduled(connection0KeepAliveTimer0, cancelContinuation: connection0KeepAliveTimer0CancellationToken)

        // keep alive timers are triggered after 30s
        stateMachine.timerTriggered(connection2KeepAliveTimer0)
        stateMachine.timerTriggered(connection1KeepAliveTimer0)
        stateMachine.timerTriggered(connection0KeepAliveTimer0)
        
        // keep alive requests are done and new timers are scheduled
        stateMachine.connectionKeepAliveDone(connection1)
        let connection1KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 1, usecase: .keepAlive), duration: .seconds(30))
        let connection1KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection1KeepAliveTimer2)
        _ = stateMachine.timerScheduled(connection1KeepAliveTimer2, cancelContinuation: connection1KeepAliveTimer2CancellationToken)
        stateMachine.connectionKeepAliveDone(connection0)
        let connection0KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 0, usecase: .keepAlive), duration: .seconds(30))
        let connection0KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection0KeepAliveTimer2)
        _ = stateMachine.timerScheduled(connection0KeepAliveTimer2, cancelContinuation: connection0KeepAliveTimer2CancellationToken)
        stateMachine.connectionKeepAliveDone(connection2)
        let connection2KeepAliveTimer2 = TestPoolStateMachine.Timer(.init(timerID: 2, connectionID: 2, usecase: .keepAlive), duration: .seconds(30))
        let connection2KeepAliveTimer2CancellationToken = MockTimerCancellationToken(connection2KeepAliveTimer2)
        _ = stateMachine.timerScheduled(connection2KeepAliveTimer2, cancelContinuation: connection2KeepAliveTimer2CancellationToken)

        // now connections might go idle or trigger another keep alive
        stateMachine.timerTriggered(connection1IdleTimer1)
        // Burst done: 1/50
        stateMachine.timerTriggered(connection1KeepAliveTimer2)
        
        // we want to start new work on all connections, but a few are occupied or closed already
        let request3 = MockRequest()
        let leaseResult = stateMachine.leaseConnection(request3) // this one works, connections are available
        XCTAssertEqual(leaseResult.request, .leaseConnection(.init(element: request3), connection0))
        XCTAssertEqual(leaseResult.connection, .none)

        // a few more timers are getting triggered
        stateMachine.timerTriggered(connection0KeepAliveTimer2)
        stateMachine.timerTriggered(connection2KeepAliveTimer2)
        stateMachine.timerTriggered(connection2IdleTimer1)
        
        let request4 = MockRequest() // we need another connection, this will cause a crash
        stateMachine.leaseConnection(request4) // it adds a request to the queue, as no connections are available
        stateMachine.timerTriggered(connection0IdleTimer1) // here the crash happens, either in idle or keep alive timer

        // The reason for the crash might be that all connections are currently unavailable:
        // 0: keep alive in progress
        // 1: marked as going away
        // 2: marked as going away

//        _ConnectionPoolModule/PoolStateMachine.swift:422: Precondition failed
    }


}
