@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct PoolStateMachine_ConnectionStateTests {

    typealias TestConnectionState = TestPoolStateMachine.ConnectionState

    let executor = NothingConnectionPoolExecutor()

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testStartupLeaseReleaseParkLease() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        #expect(state.id == connectionID)
        #expect(!state.isIdle)
        #expect(!state.isAvailable)
        #expect(!state.isConnected)
        #expect(!state.isLeased)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        #expect(state.isIdle)
        #expect(state.isAvailable)
        #expect(state.isConnected)
        #expect(state.isLeased == false)
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        #expect(!state.isIdle)
        #expect(!state.isAvailable)
        #expect(state.isConnected)
        #expect(state.isLeased)

        #expect(state.release(streams: 1) == .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        #expect(
            parkResult.elementsEqual([
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ])
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            Issue.record("Expected to get two timers")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)

        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        #expect(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken) == nil)

        let expectLeaseAction = TestConnectionState.LeaseAction(
            connection: connection,
            timersToCancel: [idleTimerCancellationToken, keepAliveTimerCancellationToken],
            wasIdle: true
        )
        #expect(state.lease(streams: 1) == expectLeaseAction)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testStartupParkLeaseBeforeTimersRegistered() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        #expect(
            parkResult ==
            [
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ]
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            Issue.record("Expected to get two timers")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == keepAliveTimerCancellationToken)
        #expect(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken) == idleTimerCancellationToken)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testStartupParkLeasePark() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        #expect(
            parkResult.elementsEqual([
                .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout)
            ])
        )

        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            Issue.record("Expected to get two timers")
            return
        }

        let initialKeepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let initialIdleTimerCancellationToken = MockTimerCancellationToken(idleTimer)
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        #expect(state.release(streams: 1) == .idle(availableStreams: 1, newIdle: true))
        #expect(
            state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true) ==
            [
                .init(timerID: 2, connectionID: connectionID, usecase: .keepAlive),
                .init(timerID: 3, connectionID: connectionID, usecase: .idleTimeout)
            ]
        )

        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: initialKeepAliveTimerCancellationToken) == initialKeepAliveTimerCancellationToken)
        #expect(state.timerScheduled(idleTimer, cancelContinuation: initialIdleTimerCancellationToken) == initialIdleTimerCancellationToken)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testStartupFailed() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let firstBackoffTimer = state.failedToConnect()
        let firstBackoffTimerCancellationToken = MockTimerCancellationToken(firstBackoffTimer)
        #expect(state.timerScheduled(firstBackoffTimer, cancelContinuation: firstBackoffTimerCancellationToken) == nil)
        #expect(state.retryConnect() == firstBackoffTimerCancellationToken)

        let secondBackoffTimer = state.failedToConnect()
        let secondBackoffTimerCancellationToken = MockTimerCancellationToken(secondBackoffTimer)
        #expect(state.retryConnect() == nil)
        #expect(
            state.timerScheduled(secondBackoffTimer, cancelContinuation: secondBackoffTimerCancellationToken) ==
            secondBackoffTimerCancellationToken
        )

        let thirdBackoffTimer = state.failedToConnect()
        let thirdBackoffTimerCancellationToken = MockTimerCancellationToken(thirdBackoffTimer)
        #expect(state.retryConnect() == nil)
        let forthBackoffTimer = state.failedToConnect()
        let forthBackoffTimerCancellationToken = MockTimerCancellationToken(forthBackoffTimer)
        #expect(
            state.timerScheduled(thirdBackoffTimer, cancelContinuation: thirdBackoffTimerCancellationToken) ==
            thirdBackoffTimerCancellationToken
        )
        #expect(
            state.timerScheduled(forthBackoffTimer, cancelContinuation: forthBackoffTimerCancellationToken) == nil
        )
        #expect(state.retryConnect() == forthBackoffTimerCancellationToken)

        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeaseMultipleStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected to get a keepAliveTimer")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        #expect(
            state.lease(streams: 30) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [keepAliveTimerCancellationToken], wasIdle: true)
        )

        #expect(state.release(streams: 10) == .leased(availableStreams: 80))

        #expect(
            state.lease(streams: 40) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        #expect(
            state.lease(streams: 40) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        #expect(state.release(streams: 1) == .leased(availableStreams: 1))
        #expect(state.release(streams: 98) == .leased(availableStreams: 99))
        #expect(state.release(streams: 1) == .idle(availableStreams: 100, newIdle: true))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRunningKeepAliveReducesAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected to get a keepAliveTimer")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: true) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        #expect(
            state.lease(streams: 30) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: true)
        )

        #expect(state.release(streams: 10) == .leased(availableStreams: 79))
        #expect(state.isAvailable)
        #expect(
            state.lease(streams: 79) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )
        #expect(!state.isAvailable)
        #expect(state.keepAliveSucceeded() == .leased(availableStreams: 1))
        #expect(state.isAvailable)
    }

    func testRunningKeepAliveDoesNotReduceAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected to get a keepAliveTimer")
            return
        }

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: false) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        #expect(
            state.lease(streams: 30) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: true)
        )

        #expect(state.release(streams: 10) == .leased(availableStreams: 80))
        #expect(state.keepAliveSucceeded() == .leased(availableStreams: 80))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRunKeepAliveRacesAgainstIdleClose() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID, executor: self.executor)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            Issue.record("Expected to get two timers")
            return
        }

        #expect(keepAliveTimer == .init(timerID: 0, connectionID: connectionID, usecase: .keepAlive))
        #expect(idleTimer == .init(timerID: 1, connectionID: connectionID, usecase: .idleTimeout))

        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)

        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        #expect(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken) == nil)

        #expect(state.closeIfIdle() == .init(connection: connection, previousConnectionState: .idle, cancelTimers: [keepAliveTimerCancellationToken, idleTimerCancellationToken], usedStreams: 0, maxStreams: 1, runningKeepAlive: false))
        #expect(state.runKeepAliveIfIdle(reducesAvailableStreams: true) == .none)
    }
}
