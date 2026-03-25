import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine {

    @usableFromInline
    struct KeepAliveAction {
        @usableFromInline
        var connection: Connection
        @usableFromInline
        var keepAliveTimerCancellationContinuation: TimerCancellationToken?

        @inlinable
        init(connection: Connection, keepAliveTimerCancellationContinuation: TimerCancellationToken? = nil) {
            self.connection = connection
            self.keepAliveTimerCancellationContinuation = keepAliveTimerCancellationContinuation
        }
    }

    @usableFromInline
    struct ConnectionTimer: Hashable, Sendable {
        @usableFromInline
        enum Usecase: Hashable, Sendable {
            case backoff
            case keepAlive
            case idleTimeout
        }

        @usableFromInline
        var timerID: Int

        @usableFromInline
        var connectionID: ConnectionID

        @usableFromInline
        var usecase: Usecase

        @inlinable
        init(timerID: Int, connectionID: ConnectionID, usecase: Usecase) {
            self.timerID = timerID
            self.connectionID = connectionID
            self.usecase = usecase
        }
    }

    @usableFromInline
    /// A connection state machine for the pool's view on the connection.
    struct ConnectionState: Sendable {
        @usableFromInline
        enum State: Sendable {
            @usableFromInline
            enum KeepAlive: Sendable {
                case notScheduled
                case scheduled(Timer)
                case running(_ consumingStream: Bool)

                @inlinable
                var usedStreams: UInt16 {
                    switch self {
                    case .notScheduled, .scheduled, .running(false):
                        return 0
                    case .running(true):
                        return 1
                    }
                }

                @inlinable
                var isRunning: Bool {
                    switch self {
                    case .running:
                        return true
                    case .notScheduled, .scheduled:
                        return false
                    }
                }

                @inlinable
                mutating func cancelTimerIfScheduled() -> TimerCancellationToken? {
                    switch self {
                    case .scheduled(let timer):
                        self = .notScheduled
                        return timer.cancellationContinuation
                    case .running, .notScheduled:
                        return nil
                    }
                }
            }

            @usableFromInline
            struct Timer: Sendable {
                @usableFromInline
                let timerID: Int

                @usableFromInline
                private(set) var cancellationContinuation: TimerCancellationToken?

                @inlinable
                init(id: Int) {
                    self.timerID = id
                    self.cancellationContinuation = nil
                }

                @inlinable
                mutating func registerCancellationContinuation(_ continuation: TimerCancellationToken) {
                    precondition(self.cancellationContinuation == nil)
                    self.cancellationContinuation = continuation
                }
            }

            /// The pool is creating a connection. Valid transitions are to: `.backingOff`, `.idle`, and `.closed`.
            case starting
            /// The pool is waiting to retry establishing a connection. Valid transitions are to: `.closed`.
            /// This means, the connection can be removed from the connections without cancelling external
            /// state. The connection state can then be replaced by a new one.
            case backingOff(Timer)
            /// The connection is `idle` and ready to execute a new query. Valid transitions to: `.leased`,
            /// `.closing`, and `.closed`.
            case idle(Connection, maxStreams: UInt16, keepAlive: KeepAlive, idleTimer: Timer?, closeContinuation: ConnectionCloseToken)
            /// The connection is leased and executing a query. Valid transitions to: `.idle` and `.closed`
            case leased(Connection, usedStreams: UInt16, maxStreams: UInt16, keepAlive: KeepAlive, closeContinuation: ConnectionCloseToken)
            /// The connection is closing. Valid transitions to: `.closed`
            case closing(Connection)
            /// The connection is closed. Final state.
            case closed
        }

        @usableFromInline
        let id: ConnectionID

        @usableFromInline
        private(set) var state: State = .starting

        @usableFromInline
        private(set) var nextTimerID: Int = 0

        @inlinable
        init(id: ConnectionID) {
            self.id = id
        }

        @inlinable
        var isIdle: Bool {
            switch self.state {
            case .idle(_, _, .notScheduled, _, _), .idle(_, _, .scheduled, _, _):
                return true
            case .idle(_, _, .running, _, _):
                return false
            case .backingOff, .starting, .closed, .closing, .leased:
                return false
            }
        }

        @inlinable
        var isAvailable: Bool {
            switch self.state {
            case .idle(_, let maxStreams, .running(true), _, _):
                return maxStreams > 1
            case .idle(_, let maxStreams, let keepAlive, _, _):
                return keepAlive.usedStreams < maxStreams
            case .leased(_, let usedStreams, let maxStreams, let keepAlive, _):
                return usedStreams + keepAlive.usedStreams < maxStreams
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        @inlinable
        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .closed, .closing, .idle:
                return false
            }
        }

        @inlinable
        var isConnected: Bool {
            switch self.state {
            case .idle, .leased:
                return true
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        @inlinable
        var isClosed: Bool {
            switch self.state {
            case .closed:
                return true
            case .idle, .leased, .backingOff, .starting, .closing:
                return false
            }
        }

        @inlinable
        mutating func connected(_ connection: Connection, maxStreams: UInt16, closeContinuation: ConnectionCloseToken) -> ConnectionAvailableInfo {
            switch self.state {
            case .starting:
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .notScheduled, idleTimer: nil, closeContinuation: closeContinuation)
                return .idle(availableStreams: maxStreams, newIdle: true)
            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @usableFromInline
        struct NewMaxStreamInfo {
            @usableFromInline
            var newMaxStreams: UInt16

            @usableFromInline
            var oldMaxStreams: UInt16

            @usableFromInline
            var usedStreams: UInt16

            @inlinable
            init(newMaxStreams: UInt16, oldMaxStreams: UInt16, usedStreams: UInt16) {
                self.newMaxStreams = newMaxStreams
                self.oldMaxStreams = oldMaxStreams
                self.usedStreams = usedStreams
            }
        }

        @inlinable
        mutating func newMaxStreamSetting(_ newMaxStreams: UInt16) -> NewMaxStreamInfo? {
            switch self.state {
            case .starting, .backingOff:
                preconditionFailure("Invalid state: \(self.state)")

            case .idle(let connection, let oldMaxStreams, let keepAlive, idleTimer: let idleTimer, let closeContinuation):
                self.state = .idle(connection, maxStreams: newMaxStreams, keepAlive: keepAlive, idleTimer: idleTimer, closeContinuation: closeContinuation)
                return NewMaxStreamInfo(
                    newMaxStreams: newMaxStreams,
                    oldMaxStreams: oldMaxStreams,
                    usedStreams: keepAlive.usedStreams
                )

            case .leased(let connection, let usedStreams, let oldMaxStreams, let keepAlive, let closeContinuation):
                self.state = .leased(connection, usedStreams: usedStreams, maxStreams: newMaxStreams, keepAlive: keepAlive, closeContinuation: closeContinuation)
                return NewMaxStreamInfo(
                    newMaxStreams: newMaxStreams,
                    oldMaxStreams: oldMaxStreams,
                    usedStreams: usedStreams + keepAlive.usedStreams
                )

            case .closing, .closed:
                return nil
            }
        }

        @inlinable
        mutating func parkConnection(scheduleKeepAliveTimer: Bool, scheduleIdleTimeoutTimer: Bool) -> Max2Sequence<ConnectionTimer> {
            var keepAliveTimer: ConnectionTimer?
            var keepAliveTimerState: State.Timer?
            var idleTimer: ConnectionTimer?
            var idleTimerState: State.Timer?

            switch self.state {
            case .backingOff, .starting, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")

            case .idle(let connection, let maxStreams, .notScheduled, .none, let closeContinuation):
                let keepAlive: State.KeepAlive
                if scheduleKeepAliveTimer {
                    keepAliveTimerState = self._nextTimer()
                    keepAliveTimer = ConnectionTimer(timerID: keepAliveTimerState!.timerID, connectionID: self.id, usecase: .keepAlive)
                    keepAlive = .scheduled(keepAliveTimerState!)
                } else {
                    keepAlive = .notScheduled
                }
                if scheduleIdleTimeoutTimer {
                    idleTimerState = self._nextTimer()
                    idleTimer = ConnectionTimer(timerID: idleTimerState!.timerID, connectionID: self.id, usecase: .idleTimeout)
                }
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: idleTimerState, closeContinuation: closeContinuation)
                return Max2Sequence(keepAliveTimer, idleTimer)

            case .idle(_, _, .scheduled, .some, _):
                precondition(!scheduleKeepAliveTimer)
                precondition(!scheduleIdleTimeoutTimer)
                return Max2Sequence()

            case .idle(let connection, let maxStreams, .notScheduled, let idleTimer, let closeContinuation):
                precondition(!scheduleIdleTimeoutTimer)
                let keepAlive: State.KeepAlive
                if scheduleKeepAliveTimer {
                    keepAliveTimerState = self._nextTimer()
                    keepAliveTimer = ConnectionTimer(timerID: keepAliveTimerState!.timerID, connectionID: self.id, usecase: .keepAlive)
                    keepAlive = .scheduled(keepAliveTimerState!)
                } else {
                    keepAlive = .notScheduled
                }
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: idleTimer, closeContinuation: closeContinuation)
                return Max2Sequence(keepAliveTimer)

            case .idle(let connection, let maxStreams, .scheduled(let keepAliveTimer), .none, let closeContinuation):
                precondition(!scheduleKeepAliveTimer)

                if scheduleIdleTimeoutTimer {
                    idleTimerState = self._nextTimer()
                    idleTimer = ConnectionTimer(timerID: idleTimerState!.timerID, connectionID: self.id, usecase: .idleTimeout)
                }
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .scheduled(keepAliveTimer), idleTimer: idleTimerState, closeContinuation: closeContinuation)
                return Max2Sequence(idleTimer, nil)

            case .idle(let connection, let maxStreams, keepAlive: .running(let usingStream), idleTimer: .none, let closeContinuation):
                if scheduleIdleTimeoutTimer {
                    idleTimerState = self._nextTimer()
                    idleTimer = ConnectionTimer(timerID: idleTimerState!.timerID, connectionID: self.id, usecase: .idleTimeout)
                }
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .running(usingStream), idleTimer: idleTimerState, closeContinuation: closeContinuation)
                return Max2Sequence(keepAliveTimer, idleTimer)

            case .idle(_, _, keepAlive: .running(_), idleTimer: .some, _):
                precondition(!scheduleKeepAliveTimer)
                precondition(!scheduleIdleTimeoutTimer)
                return Max2Sequence()
            }
        }

        @inlinable
        mutating func rescheduleIdleTimer() -> ConnectionTimer? {
            switch self.state {
            case .backingOff, .starting:
                preconditionFailure("Invalid state: \(self.state)")

            case .idle(let connection, let maxStreams, let keepAlive, .some, let closeContinuation):
                let idleTimerState = self._nextTimer()
                let idleTimer = ConnectionTimer(timerID: idleTimerState.timerID, connectionID: self.id, usecase: .idleTimeout)
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: idleTimerState, closeContinuation: closeContinuation)
                return idleTimer

            case .idle(_,_,_, .none, _):
                return nil
                
            case .leased, .closing, .closed:
                return nil
            }
        }

        /// The connection failed to start
        @inlinable
        mutating func failedToConnect() -> ConnectionTimer {
            switch self.state {
            case .starting:
                let backoffTimerState = self._nextTimer()
                self.state = .backingOff(backoffTimerState)
                return ConnectionTimer(timerID: backoffTimerState.timerID, connectionID: self.id, usecase: .backoff)

            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        /// Moves a connection, that has previously ``failedToConnect()`` back into the connecting state.
        ///
        /// - Returns: A ``TimerCancellationToken`` that was previously registered with the state machine
        ///            for the ``ConnectionTimer`` returned in ``failedToConnect()``. If no token was registered
        ///            nil is returned.
        @inlinable
        mutating func retryConnect() -> TimerCancellationToken? {
            switch self.state {
            case .backingOff(let timer):
                self.state = .starting
                return timer.cancellationContinuation
            case .starting, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func destroyBackingOffConnection() -> TimerCancellationToken? {
            switch self.state {
            case .backingOff(let timer):
                self.state = .closed
                return timer.cancellationContinuation
            case .starting, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @usableFromInline
        struct LeaseAction {
            @usableFromInline
            var connection: Connection
            @usableFromInline
            var connectionID: ConnectionID
            @usableFromInline
            var timersToCancel: Max2Sequence<TimerCancellationToken>
            @usableFromInline
            var wasIdle: Bool

            @inlinable
            init(connection: Connection, connectionID: ConnectionID, timersToCancel: Max2Sequence<TimerCancellationToken>, wasIdle: Bool) {
                self.connection = connection
                self.connectionID = connectionID
                self.timersToCancel = timersToCancel
                self.wasIdle = wasIdle
            }
        }

        @inlinable
        mutating func lease(streams newLeasedStreams: UInt16 = 1) -> LeaseAction {
            switch self.state {
            case .idle(let connection, let maxStreams, var keepAlive, let idleTimer, let closeContinuation):
                var cancel = Max2Sequence<TimerCancellationToken>()
                if let token = idleTimer?.cancellationContinuation {
                    cancel.append(token)
                }
                if let token = keepAlive.cancelTimerIfScheduled() {
                    cancel.append(token)
                }
                precondition(maxStreams >= newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive, closeContinuation: closeContinuation)
                return LeaseAction(connection: connection, connectionID: self.id, timersToCancel: cancel, wasIdle: true)

            case .leased(let connection, let usedStreams, let maxStreams, let keepAlive, let closeContinuation):
                precondition(maxStreams >= usedStreams + newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: usedStreams + newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive, closeContinuation: closeContinuation)
                return LeaseAction(connection: connection, connectionID: self.id, timersToCancel: .init(), wasIdle: false)

            case .backingOff, .starting, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func release(streams returnedStreams: UInt16) -> ConnectionAvailableInfo? {
            switch self.state {
            case .leased(let connection, let usedStreams, let maxStreams, let keepAlive, let closeContinuation):
                precondition(usedStreams >= returnedStreams)
                let newUsedStreams = usedStreams - returnedStreams
                let availableStreams = maxStreams - (newUsedStreams + keepAlive.usedStreams)
                if newUsedStreams == 0 {
                    self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: nil, closeContinuation: closeContinuation)
                    return .idle(availableStreams: availableStreams, newIdle: true)
                } else {
                    self.state = .leased(connection, usedStreams: newUsedStreams, maxStreams: maxStreams, keepAlive: keepAlive, closeContinuation: closeContinuation)
                    return .leased(availableStreams: availableStreams)
                }

            case .closing:
                return nil

            case .backingOff, .starting, .idle, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func runKeepAliveIfIdle(reducesAvailableStreams: Bool) -> KeepAliveAction? {
            switch self.state {
            case .idle(let connection, let maxStreams, .scheduled(let timer), let idleTimer, let closeContinuation):
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .running(reducesAvailableStreams), idleTimer: idleTimer, closeContinuation: closeContinuation)
                return KeepAliveAction(
                    connection: connection,
                    keepAliveTimerCancellationContinuation: timer.cancellationContinuation
                )

            case .leased, .closed, .closing:
                return nil

            case .backingOff, .starting, .idle(_, _, .running, _, _), .idle(_, _, .notScheduled, _, _):
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func keepAliveSucceeded() -> ConnectionAvailableInfo? {
            switch self.state {
            case .idle(let connection, let maxStreams, .running, let idleTimer, let closeContinuation):
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .notScheduled, idleTimer: idleTimer, closeContinuation: closeContinuation)
                return .idle(availableStreams: maxStreams, newIdle: false)

            case .leased(let connection, let usedStreams, let maxStreams, .running, let closeContinuation):
                self.state = .leased(connection, usedStreams: usedStreams, maxStreams: maxStreams, keepAlive: .notScheduled, closeContinuation: closeContinuation)
                return .leased(availableStreams: maxStreams - usedStreams)

            case .closed, .closing:
                return nil

            case .backingOff, .starting,
                 .leased(_, _, _, .notScheduled, _),
                 .leased(_, _, _, .scheduled, _),
                 .idle(_, _, .notScheduled, _, _),
                 .idle(_, _, .scheduled, _, _):
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func keepAliveFailed() -> CloseAction? {
            return self.close()
        }

        @inlinable
        mutating func timerScheduled(
            _ timer: ConnectionTimer,
            cancelContinuation: TimerCancellationToken
        ) -> TimerCancellationToken? {
            switch timer.usecase {
            case .backoff:
                switch self.state {
                case .backingOff(var timerState):
                    if timerState.timerID == timer.timerID {
                        timerState.registerCancellationContinuation(cancelContinuation)
                        self.state = .backingOff(timerState)
                        return nil
                    } else {
                        return cancelContinuation
                    }

                case .starting, .idle, .leased, .closing, .closed:
                    return cancelContinuation
                }

            case .idleTimeout:
                switch self.state {
                case .idle(let connection, let maxStreams, let keepAlive, let idleTimerState, let closeContinuation):
                    if var idleTimerState = idleTimerState, idleTimerState.timerID == timer.timerID {
                        idleTimerState.registerCancellationContinuation(cancelContinuation)
                        self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: idleTimerState, closeContinuation: closeContinuation)
                        return nil
                    } else {
                        return cancelContinuation
                    }

                case .starting, .backingOff, .leased, .closing, .closed:
                    return cancelContinuation
                }

            case .keepAlive:
                switch self.state {
                case .idle(let connection, let maxStreams, .scheduled(var keepAliveTimerState), let idleTimerState, let closeContinuation):
                    if keepAliveTimerState.timerID == timer.timerID {
                        keepAliveTimerState.registerCancellationContinuation(cancelContinuation)
                        self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .scheduled(keepAliveTimerState), idleTimer: idleTimerState, closeContinuation: closeContinuation)
                        return nil
                    } else {
                        return cancelContinuation
                    }

                case .starting, .backingOff, .leased, .closing, .closed,
                     .idle(_, _, .running, _, _),
                     .idle(_, _, .notScheduled, _, _):
                    return cancelContinuation
                }
            }
        }

        @inlinable
        mutating func cancelIdleTimer() -> TimerCancellationToken? {
            switch self.state {
            case .starting, .backingOff, .leased, .closing, .closed:
                return nil

            case .idle(let connection, let maxStreams, let keepAlive, let idleTimer, let closeContinuation):
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive, idleTimer: nil, closeContinuation: closeContinuation)
                return idleTimer?.cancellationContinuation
            }
        }

        @usableFromInline
        struct CloseAction {

            @usableFromInline
            enum PreviousConnectionState {
                case idle
                case leased
                case closing
                case backingOff
            }

            @usableFromInline
            var connection: Connection?
            @usableFromInline
            var previousConnectionState: PreviousConnectionState
            @usableFromInline
            var cancelTimers: Max2Sequence<TimerCancellationToken>
            @usableFromInline
            var usedStreams: UInt16
            @usableFromInline
            var maxStreams: UInt16
            @usableFromInline
            var runningKeepAlive: Bool
            @usableFromInline
            var closeContinuation: ConnectionCloseToken?


            @inlinable
            init(
                connection: Connection?,
                previousConnectionState: PreviousConnectionState,
                cancelTimers: Max2Sequence<TimerCancellationToken>,
                usedStreams: UInt16,
                maxStreams: UInt16,
                runningKeepAlive: Bool,
                closeContinuation: ConnectionCloseToken? = nil
            ) {
                self.connection = connection
                self.previousConnectionState = previousConnectionState
                self.cancelTimers = cancelTimers
                self.usedStreams = usedStreams
                self.maxStreams = maxStreams
                self.runningKeepAlive = runningKeepAlive
                self.closeContinuation = closeContinuation
            }
        }

        @inlinable
        mutating func closeIfIdle() -> CloseAction? {
            switch self.state {
            case .idle(let connection, let maxStreams, var keepAlive, let idleTimerState, let closeContinuation):
                self.state = .closing(connection)
                return CloseAction(
                    connection: connection,
                    previousConnectionState: .idle,
                    cancelTimers: Max2Sequence(
                        keepAlive.cancelTimerIfScheduled(),
                        idleTimerState?.cancellationContinuation
                    ),
                    usedStreams: keepAlive.usedStreams,
                    maxStreams: maxStreams,
                    runningKeepAlive: keepAlive.isRunning,
                    closeContinuation: closeContinuation
                )

            case .leased, .closed, .closing:
                return nil

            case .backingOff, .starting:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func destroyFailedConnection() {
            switch self.state {
            case .starting:
                self.state = .closed

            case .idle, .leased, .closed, .closing, .backingOff:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func close() -> CloseAction? {
            switch self.state {
            case .starting:
                // If we are currently starting, there is nothing we can do about it right now.
                // Only once the connection has come up, or failed, we can actually act.
                return nil

            case .closing, .closed:
                // If we are already closing, we can't do anything else.
                return nil

            case .idle(let connection, let maxStreams, var keepAlive, let idleTimerState, let closeContinuation):
                self.state = .closing(connection)
                return CloseAction(
                    connection: connection,
                    previousConnectionState: .idle,
                    cancelTimers: Max2Sequence(
                        keepAlive.cancelTimerIfScheduled(),
                        idleTimerState?.cancellationContinuation
                    ),
                    usedStreams: keepAlive.usedStreams,
                    maxStreams: maxStreams,
                    runningKeepAlive: keepAlive.isRunning,
                    closeContinuation: closeContinuation
                )

            case .leased(let connection, usedStreams: let usedStreams, maxStreams: let maxStreams, var keepAlive, let closeContinuation):
                self.state = .closing(connection)
                return CloseAction(
                    connection: connection,
                    previousConnectionState: .leased,
                    cancelTimers: Max2Sequence(
                        keepAlive.cancelTimerIfScheduled()
                    ),
                    usedStreams: keepAlive.usedStreams + usedStreams,
                    maxStreams: maxStreams,
                    runningKeepAlive: keepAlive.isRunning,
                    closeContinuation: closeContinuation
                )

            case .backingOff(let timer):
                self.state = .closed
                return CloseAction(
                    connection: nil,
                    previousConnectionState: .backingOff,
                    cancelTimers: Max2Sequence(timer.cancellationContinuation),
                    usedStreams: 0,
                    maxStreams: 0,
                    runningKeepAlive: false
                )
            }
        }

        @usableFromInline
        struct ClosedAction {

            @usableFromInline
            enum PreviousConnectionState {
                case idle
                case leased
                case closing
            }

            @usableFromInline
            var previousConnectionState: PreviousConnectionState
            @usableFromInline
            var cancelTimers: Max2Sequence<TimerCancellationToken>
            @usableFromInline
            var maxStreams: UInt16
            @usableFromInline
            var usedStreams: UInt16
            @usableFromInline
            var wasRunningKeepAlive: Bool

            @inlinable
            init(
                previousConnectionState: PreviousConnectionState,
                cancelTimers: Max2Sequence<TimerCancellationToken>,
                maxStreams: UInt16,
                usedStreams: UInt16,
                wasRunningKeepAlive: Bool
            ) {
                self.previousConnectionState = previousConnectionState
                self.cancelTimers = cancelTimers
                self.maxStreams = maxStreams
                self.usedStreams = usedStreams
                self.wasRunningKeepAlive = wasRunningKeepAlive
            }
        }

        @inlinable
        mutating func closed() -> ClosedAction {
            switch self.state {
            case .starting, .backingOff, .closed:
                preconditionFailure("Invalid state: \(self.state)")

            case .idle(_, let maxStreams, var keepAlive, let idleTimer, _):
                self.state = .closed
                return ClosedAction(
                    previousConnectionState: .idle,
                    cancelTimers: .init(keepAlive.cancelTimerIfScheduled(), idleTimer?.cancellationContinuation),
                    maxStreams: maxStreams,
                    usedStreams: keepAlive.usedStreams,
                    wasRunningKeepAlive: keepAlive.isRunning
                )

            case .leased(_, let usedStreams, let maxStreams, let keepAlive, _):
                self.state = .closed
                return ClosedAction(
                    previousConnectionState: .leased,
                    cancelTimers: .init(),
                    maxStreams: maxStreams,
                    usedStreams: usedStreams + keepAlive.usedStreams,
                    wasRunningKeepAlive: keepAlive.isRunning
                )

            case .closing:
                self.state = .closed
                return ClosedAction(
                    previousConnectionState: .closing,
                    cancelTimers: .init(),
                    maxStreams: 0,
                    usedStreams: 0,
                    wasRunningKeepAlive: false
                )
            }
        }

        // MARK: - Private Methods -

        @inlinable
        mutating /*private*/ func _nextTimer() -> State.Timer {
            defer { self.nextTimerID += 1 }
            return State.Timer(id: self.nextTimerID)
        }
    }

    @usableFromInline
    enum ConnectionAvailableInfo: Equatable {
        case leased(availableStreams: UInt16)
        case idle(availableStreams: UInt16, newIdle: Bool)

        @usableFromInline
        var availableStreams: UInt16 {
            switch self {
            case .leased(let availableStreams):
                return availableStreams
            case .idle(let availableStreams, newIdle: _):
                return availableStreams
            }
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.KeepAliveAction: Equatable where TimerCancellationToken: Equatable {
    @inlinable
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.connection === rhs.connection && lhs.keepAliveTimerCancellationContinuation == rhs.keepAliveTimerCancellationContinuation
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionState.LeaseAction: Equatable where TimerCancellationToken: Equatable {
    @inlinable
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.wasIdle == rhs.wasIdle && lhs.connection === rhs.connection && lhs.timersToCancel == rhs.timersToCancel
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionState.CloseAction: Equatable where TimerCancellationToken: Equatable {
    @inlinable
    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.cancelTimers == rhs.cancelTimers && lhs.connection === rhs.connection && lhs.maxStreams == rhs.maxStreams
    }
}
