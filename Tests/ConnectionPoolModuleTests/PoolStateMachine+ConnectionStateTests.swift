@testable import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Testing

@Suite struct PoolStateMachine_ConnectionStateTests {

    typealias TestConnectionState = TestPoolStateMachine.ConnectionState

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testStartupLeaseReleaseParkLease() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        #expect(state.id == connectionID)
        #expect(!state.isIdle)
        #expect(!state.isAvailable)
        #expect(!state.isConnected)
        #expect(!state.isLeased)
        let connection = MockConnection(id: connectionID)
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

        #expect(state.release(streams: 1) == .available(.idle(availableStreams: 1, newIdle: true)))
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
        let connection = MockConnection(id: connectionID)
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
        let connection = MockConnection(id: connectionID)
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

        #expect(state.release(streams: 1) == .available(.idle(availableStreams: 1, newIdle: true)))
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

        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testLeaseMultipleStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
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

        #expect(state.release(streams: 10) == .available(.leased(availableStreams: 80)))

        #expect(
            state.lease(streams: 40) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        #expect(
            state.lease(streams: 40) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )

        #expect(state.release(streams: 1) == .available(.leased(availableStreams: 1)))
        #expect(state.release(streams: 98) == .available(.leased(availableStreams: 99)))
        #expect(state.release(streams: 1) == .available(.idle(availableStreams: 100, newIdle: true)))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRunningKeepAliveReducesAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
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

        #expect(state.release(streams: 10) == .available(.leased(availableStreams: 79)))
        #expect(state.isAvailable)
        #expect(
            state.lease(streams: 79) ==
            TestConnectionState.LeaseAction(connection: connection, timersToCancel: [], wasIdle: false)
        )
        #expect(!state.isAvailable)
        #expect(state.keepAliveSucceeded() == .leased(availableStreams: 1))
        #expect(state.isAvailable)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRunningKeepAliveDoesNotReduceAvailableStreams() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
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

        #expect(state.release(streams: 10) == .available(.leased(availableStreams: 80)))
        #expect(state.keepAliveSucceeded() == .leased(availableStreams: 80))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testRunKeepAliveRacesAgainstIdleClose() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
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

    // MARK: - markForClose tests

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMarkForCloseOnIdleConnection() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))

        let parkResult = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: true)
        guard let keepAliveTimer = parkResult.first, let idleTimer = parkResult.second else {
            Issue.record("Expected to get two timers")
            return
        }
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        let idleTimerCancellationToken = MockTimerCancellationToken(idleTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        #expect(state.timerScheduled(idleTimer, cancelContinuation: idleTimerCancellationToken) == nil)

        guard case .closeConnection(let closeAction) = state.markForClose() else {
            Issue.record("Expected closeConnection action for idle connection")
            return
        }
        #expect(closeAction.connection === connection)
        #expect(closeAction.previousConnectionState == .idle)
        #expect(!state.isAvailable)
        #expect(!state.isIdle)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMarkForCloseOnLeasedConnection() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))
        #expect(state.lease(streams: 2) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        guard case .markedForClose(availableStreams: let availableStreams, keepAliveWasRunning: let keepAliveWasRunning) = state.markForClose() else {
            Issue.record("Expected markedForClose action for leased connection")
            return
        }
        #expect(availableStreams == 2) // maxStreams(4) - usedStreams(2) - keepAlive(0)
        #expect(keepAliveWasRunning == false)
        #expect(!state.isAvailable)
        #expect(state.isLeased)
        #expect(state.isDraining)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMarkForCloseOnClosingConnection() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        _ = state.closeIfIdle()

        guard case .alreadyClosing = state.markForClose() else {
            Issue.record("Expected alreadyClosing for closing connection")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testReleaseAfterMarkForClose() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        guard case .markedForClose(availableStreams: let availableStreams, keepAliveWasRunning: let keepAliveWasRunning) = state.markForClose() else {
            Issue.record("Expected markedForClose")
            return
        }
        #expect(availableStreams == 0) // fully used
        #expect(keepAliveWasRunning == false)

        // Release all streams — should transition to closing
        #expect(state.release(streams: 1) == .drainingComplete(connection))
        #expect(!state.isLeased)
        #expect(!state.isAvailable)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testMarkForCloseOnLeasedWithKeepAliveRunning_ReleaseDrainsWithoutWaitingForKeepAlive() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))

        // Park and start keepAlive
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keepAliveTimer")
            return
        }
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        // Start keepAlive
        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: true) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        // Lease while keepAlive is running
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        // Mark for close — keepAlive is running
        guard case .markedForClose(availableStreams: let availableStreams, keepAliveWasRunning: let keepAliveWasRunning) = state.markForClose() else {
            Issue.record("Expected markedForClose")
            return
        }
        #expect(availableStreams == 98) // maxStreams(100) - usedStreams(1) - keepAlive(1)
        #expect(keepAliveWasRunning == true)
        #expect(state.isDraining)

        // Release all streams — should transition to closing immediately (don't wait for keepAlive)
        #expect(state.release(streams: 1) == .drainingComplete(connection))
        #expect(!state.isLeased)
        #expect(!state.isDraining)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveSucceededOnDrainingIsNoOp() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))

        // Park and start keepAlive
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keepAliveTimer")
            return
        }
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: true) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        // Lease while keepAlive is running
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        // Mark for close
        guard case .markedForClose = state.markForClose() else {
            Issue.record("Expected markedForClose")
            return
        }
        #expect(state.isDraining)

        // keepAliveSucceeded on draining → no state change, still draining
        #expect(state.keepAliveSucceeded() == nil)
        #expect(state.isDraining)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testKeepAliveFailedOnDrainingForcesClose() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 100) == .idle(availableStreams: 100, newIdle: true))

        // Park and start keepAlive
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keepAliveTimer")
            return
        }
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)

        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: false) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        // Lease while keepAlive is running
        #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        // Mark for close
        guard case .markedForClose = state.markForClose() else {
            Issue.record("Expected markedForClose")
            return
        }
        #expect(state.isDraining)

        // keepAliveFailed on draining → force close to closing
        let closeAction = state.keepAliveFailed()
        #expect(closeAction != nil)
        #expect(closeAction?.connection === connection)
        #expect(closeAction?.previousConnectionState == .leased)
        #expect(!state.isDraining)
    }

    // MARK: - newMaxStreamSetting tests

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test(arguments: [(4, 8), (8, 4)])
    func changeMaxStreamsOnIdleConnection(initial: UInt16, update: UInt16) {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: initial) == .idle(availableStreams: initial, newIdle: true))

        let info = state.newMaxStreamSetting(update)
        #expect(info != nil)
        #expect(info?.newMaxStreams == update)
        #expect(info?.oldMaxStreams == initial)
        #expect(info?.usedStreams == 0)
        #expect(state.isAvailable)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func increaseMaxStreamsOnLeasedConnection() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))
        #expect(state.lease(streams: 3) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        let info = state.newMaxStreamSetting(8)
        #expect(info != nil)
        #expect(info?.newMaxStreams == 8)
        #expect(info?.oldMaxStreams == 4)
        #expect(info?.usedStreams == 3)
        #expect(state.isAvailable)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testNewMaxStreamSettingOnDrainingClosingClosedReturnsNil() {
        // draining
        do {
            let connectionID = 1
            var state = TestConnectionState(id: connectionID)
            let connection = MockConnection(id: connectionID)
            #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))
            #expect(state.lease(streams: 1) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))
            _ = state.markForClose()
            #expect(state.isDraining)
            #expect(state.newMaxStreamSetting(8) == nil)
        }

        // closing
        do {
            let connectionID = 2
            var state = TestConnectionState(id: connectionID)
            let connection = MockConnection(id: connectionID)
            #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
            _ = state.closeIfIdle()
            #expect(state.newMaxStreamSetting(8) == nil)
        }

        // closed
        do {
            let connectionID = 3
            var state = TestConnectionState(id: connectionID)
            let connection = MockConnection(id: connectionID)
            #expect(state.connected(connection, maxStreams: 1) == .idle(availableStreams: 1, newIdle: true))
            _ = state.closeIfIdle()
            _ = state.closed()
            #expect(state.isClosed)
            #expect(state.newMaxStreamSetting(8) == nil)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testIncreaseMaxStreamsWhileKeepAliveRunning() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))
        let timers = state.parkConnection(scheduleKeepAliveTimer: true, scheduleIdleTimeoutTimer: false)
        guard let keepAliveTimer = timers.first else {
            Issue.record("Expected a keepAliveTimer")
            return
        }
        let keepAliveTimerCancellationToken = MockTimerCancellationToken(keepAliveTimer)
        #expect(state.timerScheduled(keepAliveTimer, cancelContinuation: keepAliveTimerCancellationToken) == nil)
        #expect(
            state.runKeepAliveIfIdle(reducesAvailableStreams: true) ==
            .init(connection: connection, keepAliveTimerCancellationContinuation: keepAliveTimerCancellationToken)
        )

        let info = state.newMaxStreamSetting(8)
        #expect(info != nil)
        #expect(info?.newMaxStreams == 8)
        #expect(info?.oldMaxStreams == 4)
        // keepAlive consuming a stream should be reflected in usedStreams
        #expect(info?.usedStreams == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testSetMaxStreamsToSameValue() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))

        let info = state.newMaxStreamSetting(4)
        #expect(info != nil)
        #expect(info?.newMaxStreams == 4)
        #expect(info?.oldMaxStreams == 4)
        #expect(info?.usedStreams == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test func testReleaseAfterMaxStreamsReduced() {
        let connectionID = 1
        var state = TestConnectionState(id: connectionID)
        let connection = MockConnection(id: connectionID)
        #expect(state.connected(connection, maxStreams: 4) == .idle(availableStreams: 4, newIdle: true))

        // Lease 3 out of 4 streams
        #expect(state.lease(streams: 3) == .init(connection: connection, timersToCancel: .init(), wasIdle: true))

        // Server reduces maxStreams to 2 while we hold 3
        let info = state.newMaxStreamSetting(2)
        #expect(info != nil)
        #expect(info?.newMaxStreams == 2)
        #expect(info?.oldMaxStreams == 4)

        // Release 1 stream: usedStreams goes from 3 to 2, but maxStreams is 2 so availableStreams = 0
        #expect(state.release(streams: 1) == .none)
        #expect(state.isLeased)

        // Release another: usedStreams goes from 2 to 1, availableStreams = 1
        #expect(state.release(streams: 1) == .available(.leased(availableStreams: 1)))

        // Release last: usedStreams = 0 → idle
        #expect(state.release(streams: 1) == .available(.idle(availableStreams: 2, newIdle: true)))
    }
}
