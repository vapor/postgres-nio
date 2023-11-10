@testable import _ConnectionPoolModule
import XCTest

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PoolStateMachine_ConnectionStateTests: XCTestCase {

    typealias TestConnectionState = TestPoolStateMachine.ConnectionState

    func testStartupLeaseReleaseParkLease() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        XCTAssertEqual(state.id, connectionID)
        XCTAssertEqual(state.isIdle, false)
        XCTAssertEqual(state.isAvailable, false)
        XCTAssertEqual(state.isConnected, false)
        XCTAssertEqual(state.isLeased, false)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 1), .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(state.isIdle, true)
        XCTAssertEqual(state.isAvailable, true)
        XCTAssertEqual(state.isConnected, true)
        XCTAssertEqual(state.isLeased, false)
        XCTAssertEqual(state.lease(streams: 1), .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        XCTAssertEqual(state.isIdle, false)
        XCTAssertEqual(state.isAvailable, false)
        XCTAssertEqual(state.isConnected, true)
        XCTAssertEqual(state.isLeased, true)

        XCTAssertEqual(state.release(streams: 1), .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        XCTAssert(
            parkResult.elementsEqual([
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ])
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            return XCTFail("Expected to get two timers")
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)

        XCTAssertNil(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))
        XCTAssertNil(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken))

        let expectLeaseAction = TestConnectionState.LeaseAction(
            connection: connection,
            timersToCancel: [idleTimerCancellationToken, keepAliveTimerCancellationToken],
            wasIdle: true
        )
        XCTAssertEqual(state.lease(streams: 1), expectLeaseAction)
    }

    func testStartupParkLeaseBeforeTimersRegistered() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 1), .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        XCTAssertEqual(
            parkResult,
            [
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ]
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            return XCTFail("Expected to get two timers")
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)
        XCTAssertEqual(state.lease(streams: 1), .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        XCTAssertEqual(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken), keepAliveTimerCancellationToken)
        XCTAssertEqual(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken), idleTimerCancellationToken)
    }

    func testStartupParkLeasePark() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 1), .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        XCTAssert(
            parkResult.elementsEqual([
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ])
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            return XCTFail("Expected to get two timers")
        }

        let initialKeepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let initialIdleTimerCancellationToken = MockTimerCancellationToken(idleTimer)
        XCTAssertEqual(state.lease(streams: 1), .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        XCTAssertEqual(state.release(streams: 1), .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(
            state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true),
            [
                .init(timerID: 2, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 3, connectionID: connectionID, usecase: .idleTimeout)
            ]
        )

        XCTAssertEqual(state.timerScheduled(keepAliveTimer, cancelContinuation: initialKeepAliveTimerCancellationToken), initialKeepAliveTimerCancellationToken)
        XCTAssertEqual(state.timerScheduled(idleTimer, cancelContinuation: initialIdleTimerCancellationToken), initialIdleTimerCancellationToken)
    }

    func testStartupFailed() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let firstBackoffTimer = state.failedToConnect()
        let firstBackoffTimerCancellationToken = MockTimerCancellationToken(firstBackoffTimer)
        XCTAssertNil(state.timerScheduled(firstBackoffTimer, cancelContinuation: firstBackoffTimerCancellationToken))
        XCTAssertEqual(state.retryConnect(), firstBackoffTimerCancellationToken)

        let secondBackoffTimer = state.failedToConnect()
        let secondBackoffTimerCancellationToken = MockTimerCancellationToken(secondBackoffTimer)
        XCTAssertNil(state.retryConnect())
        XCTAssertEqual(
            state.timerScheduled(secondBackoffTimer, cancelContinuation: secondBackoffTimerCancellationToken),
            secondBackoffTimerCancellationToken
        )

        let thirdBackoffTimer = state.failedToConnect()
        let thirdBackoffTimerCancellationToken = MockTimerCancellationToken(thirdBackoffTimer)
        XCTAssertNil(state.retryConnect())
        let forthBackoffTimer = state.failedToConnect()
        let forthBackoffTimerCancellationToken = MockTimerCancellationToken(forthBackoffTimer)
        XCTAssertEqual(
            state.timerScheduled(thirdBackoffTimer, cancelContinuation: thirdBackoffTimerCancellationToken),
            thirdBackoffTimerCancellationToken
        )
        XCTAssertNil(
            state.timerScheduled(forthBackoffTimer, cancelContinuation: forthBackoffTimerCancellationToken)
        )
        XCTAssertEqual(state.retryConnect(), forthBackoffTimerCancellationToken)

        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 1), .idle(availableStreams: 1, newIdle: true))
    }

    func testLeaseMultipleStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 100), .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else { return XCTFail("Expected to get a keepAliveTimer") }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        XCTAssertNil(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))

        XCTAssertEqual(
            state.lease(streams: 30),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [keepAliveTimerCancellationToken], wasIdle: true)
        )

        XCTAssertEqual(state.release(streams: 10), .leased(availableStreams: 80))

        XCTAssertEqual(
            state.lease(streams: 40),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        XCTAssertEqual(
            state.lease(streams: 40),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        XCTAssertEqual(state.release(streams: 1), .leased(availableStreams: 1))
        XCTAssertEqual(state.release(streams: 98), .leased(availableStreams: 99))
        XCTAssertEqual(state.release(streams: 1), .idle(availableStreams: 100, newIdle: true))
    }

    func testRunningKeepAliveReducesAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 100), .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else { return XCTFail("Expected to get a keepAliveTimer") }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        XCTAssertNil(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))

        XCTAssertEqual(
            state.runKeepAliveIfIdle(reducesAvailableStreams: true),
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        XCTAssertEqual(
            state.lease(streams: 30),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: true)
        )

        XCTAssertEqual(state.release(streams: 10), .leased(availableStreams: 79))
        XCTAssertEqual(state.isAvailable, true)
        XCTAssertEqual(
            state.lease(streams: 79),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )
        XCTAssertEqual(state.isAvailable, false)
        XCTAssertEqual(state.keepAliveSucceeded(), .leased(availableStreams: 1))
        XCTAssertEqual(state.isAvailable, true)
    }

    func testRunningKeepAliveDoesNotReduceAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 100), .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else { return XCTFail("Expected to get a keepAliveTimer") }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        XCTAssertNil(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))

        XCTAssertEqual(
            state.runKeepAliveIfIdle(reducesAvailableStreams: false),
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        XCTAssertEqual(
            state.lease(streams: 30),
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: true)
        )

        XCTAssertEqual(state.release(streams: 10), .leased(availableStreams: 80))
        XCTAssertEqual(state.keepAliveSucceeded(), .leased(availableStreams: 80))
    }

    func testRunKeepAliveRacesAgainstIdleClose() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        XCTAssertEqual(state.connected(connection, maxStreams: 1), .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            return XCTFail("Expected to get two timers")
        }

        XCTAssertEqual(keepAliveTimer, .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive))
        XCTAssertEqual(idleTimer, .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout))

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)

        XCTAssertNil(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken))
        XCTAssertNil(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken))

        XCTAssertEqual(state.closeIfIdle(), .init(connection: connection, previousConnectionState: .idle, cancelTimers: [keepAliveTimerCancellationToken, idleTimerCancellationToken], usedStreams: 0, maxStreams: 1, runningKeepAlive: false))
        XCTAssertEqual(state.runKeepAliveIfIdle(reducesAvailableStreams: true), .none)

    }
}
