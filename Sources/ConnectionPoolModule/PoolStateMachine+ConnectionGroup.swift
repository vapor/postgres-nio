import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine {

    @usableFromInline
    struct LeaseResult {
        @usableFromInline
        var connection: Connection
        @usableFromInline
        var timersToCancel: Max2Sequence<TimerCancellationToken>
        @usableFromInline
        var wasIdle: Bool
        @usableFromInline
        var use: ConnectionGroup.ConnectionUse

        @inlinable
        init(
            connection: Connection,
            timersToCancel: Max2Sequence<TimerCancellationToken>,
            wasIdle: Bool,
            use: ConnectionGroup.ConnectionUse
        ) {
            self.connection = connection
            self.timersToCancel = timersToCancel
            self.wasIdle = wasIdle
            self.use = use
        }
    }

    @usableFromInline
    struct ConnectionGroup: Sendable {
        @usableFromInline
        struct Stats: Hashable, Sendable {
            @usableFromInline var connecting: UInt16 = 0
            @usableFromInline var backingOff: UInt16 = 0
            @usableFromInline var idle: UInt16 = 0
            @usableFromInline var leased: UInt16 = 0
            @usableFromInline var runningKeepAlive: UInt16 = 0
            @usableFromInline var closing: UInt16 = 0

            @usableFromInline var availableStreams: UInt16 = 0
            @usableFromInline var leasedStreams: UInt16 = 0

            @usableFromInline var soonAvailable: UInt16 {
                self.connecting + self.backingOff + self.runningKeepAlive
            }

            @usableFromInline var active: UInt16 {
                self.idle + self.leased + self.connecting + self.backingOff
            }
        }

        /// The minimum number of connections
        @usableFromInline
        let minimumConcurrentConnections: Int

        /// The maximum number of preserved connections
        @usableFromInline
        let maximumConcurrentConnectionSoftLimit: Int

        /// The absolute maximum number of connections
        @usableFromInline
        let maximumConcurrentConnectionHardLimit: Int

        @usableFromInline
        let keepAlive: Bool

        @usableFromInline
        let keepAliveReducesAvailableStreams: Bool

        /// A connectionID generator.
        @usableFromInline
        let generator: ConnectionIDGenerator

        /// The connections states
        @usableFromInline
        private(set) var connections: [ConnectionState]

        @usableFromInline
        private(set) var stats = Stats()

        @inlinable
        init(
            generator: ConnectionIDGenerator,
            minimumConcurrentConnections: Int,
            maximumConcurrentConnectionSoftLimit: Int,
            maximumConcurrentConnectionHardLimit: Int,
            keepAlive: Bool,
            keepAliveReducesAvailableStreams: Bool
        ) {
            self.generator = generator
            self.connections = []
            self.minimumConcurrentConnections = minimumConcurrentConnections
            self.maximumConcurrentConnectionSoftLimit = maximumConcurrentConnectionSoftLimit
            self.maximumConcurrentConnectionHardLimit = maximumConcurrentConnectionHardLimit
            self.keepAlive = keepAlive
            self.keepAliveReducesAvailableStreams = keepAliveReducesAvailableStreams
        }

        var isEmpty: Bool {
            self.connections.isEmpty
        }

        @usableFromInline
        var canGrow: Bool {
            self.stats.active < self.maximumConcurrentConnectionHardLimit
        }

        @usableFromInline
        var soonAvailableConnections: UInt16 {
            self.stats.soonAvailable
        }

        // MARK: - Mutations -

        /// A connection's use. Is it persisted or an overflow connection?
        @usableFromInline
        enum ConnectionUse: Equatable {
            case persisted
            case demand
            case overflow
        }

        /// Information around an idle connection.
        @usableFromInline
        struct AvailableConnectionContext {
            /// The connection's use. Either general purpose or for requests with `EventLoop`
            /// requirements.
            @usableFromInline
            var use: ConnectionUse

            @usableFromInline
            var info: ConnectionAvailableInfo
        }

        /// Information around the failed/closed connection.
        struct FailedConnectionContext {
            /// Connections that are currently starting
            var connectionsStarting: Int
        }

        mutating func refillConnections() -> [ConnectionRequest] {
            let existingConnections = self.stats.active
            let missingConnection = self.minimumConcurrentConnections - Int(existingConnections)
            guard missingConnection > 0 else {
                return []
            }

            var requests = [ConnectionRequest]()
            requests.reserveCapacity(missingConnection)

            for _ in 0..<missingConnection {
                requests.append(self.createNewConnection())
            }
            return requests
        }

        // MARK: Connection creation

        @inlinable
        mutating func createNewDemandConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.minimumConcurrentConnections <= self.stats.active)
            guard self.maximumConcurrentConnectionSoftLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        @inlinable
        mutating func createNewOverflowConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.maximumConcurrentConnectionSoftLimit <= self.stats.active)
            guard self.maximumConcurrentConnectionHardLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        @inlinable
        /*private*/ mutating func createNewConnection() -> ConnectionRequest {
            precondition(self.canGrow)
            self.stats.connecting += 1
            let connectionID = self.generator.next()
            let connection = ConnectionState(id: connectionID)
            self.connections.append(connection)
            return ConnectionRequest(connectionID: connectionID)
        }

        /// A new ``Connection`` was established.
        ///
        /// This will put the connection into the idle state.
        ///
        /// - Parameter connection: The new established connection.
        /// - Returns: An index and an IdleConnectionContext to determine the next action for the now idle connection.
        ///            Call ``parkConnection(at:)``, ``leaseConnection(at:)`` or ``closeConnection(at:)``
        ///            with the supplied index after this.
        @inlinable
        mutating func newConnectionEstablished(_ connection: Connection, maxStreams: UInt16) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connection.id }) else {
                preconditionFailure("There is a new connection that we didn't request!")
            }
            self.stats.connecting -= 1
            self.stats.idle += 1
            self.stats.availableStreams += maxStreams
            let connectionInfo = self.connections[index].connected(connection, maxStreams: maxStreams)
            // TODO: If this is an overflow connection, but we are currently also creating a
            //       persisted connection, we might want to swap those.
            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        @inlinable
        mutating func backoffNextConnectionAttempt(_ connectionID: Connection.ID) -> ConnectionTimer {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("We tried to create a new connection that we know nothing about?")
            }

            self.stats.connecting -= 1
            self.stats.backingOff += 1

            return self.connections[index].failedToConnect()
        }

        @usableFromInline
        enum BackoffDoneAction {
            case createConnection(ConnectionRequest, TimerCancellationToken?)
            case cancelTimers(Max2Sequence<TimerCancellationToken>)
        }

        @inlinable
        mutating func backoffDone(_ connectionID: Connection.ID, retry: Bool) -> BackoffDoneAction {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("We tried to create a new connection that we know nothing about?")
            }

            self.stats.backingOff -= 1

            if retry || self.stats.active < self.minimumConcurrentConnections {
                self.stats.connecting += 1
                let backoffTimerCancellation = self.connections[index].retryConnect()
                return .createConnection(.init(connectionID: connectionID), backoffTimerCancellation)
            }

            let backoffTimerCancellation = self.connections[index].destroyBackingOffConnection()
            var timerCancellations = Max2Sequence(backoffTimerCancellation)

            if let timerCancellationToken = self.swapForDeletion(index: index) {
                timerCancellations.append(timerCancellationToken)
            }
            return .cancelTimers(timerCancellations)
        }

        @inlinable
        mutating func timerScheduled(
            _ timer: ConnectionTimer,
            cancelContinuation: TimerCancellationToken
        ) -> TimerCancellationToken? {
            guard let index = self.connections.firstIndex(where: { $0.id == timer.connectionID }) else {
                return cancelContinuation
            }

            return self.connections[index].timerScheduled(timer, cancelContinuation: cancelContinuation)
        }

        // MARK: Leasing and releasing

        /// Lease a connection, if an idle connection is available.
        ///
        /// - Returns: A connection to execute a request on.
        @inlinable
        mutating func leaseConnection() -> LeaseResult? {
            if self.stats.availableStreams == 0 {
                return nil
            }

            guard let index = self.findAvailableConnection() else {
                preconditionFailure("Stats and actual count are of.")
            }

            return self.leaseConnection(at: index, streams: 1)
        }

        @usableFromInline
        enum LeasedConnectionOrStartingCount {
            case leasedConnection(LeaseResult)
            case startingCount(UInt16)
        }

        @inlinable
        mutating func leaseConnectionOrSoonAvailableConnectionCount() -> LeasedConnectionOrStartingCount {
            if let result = self.leaseConnection() {
                return .leasedConnection(result)
            }
            return .startingCount(self.stats.soonAvailable)
        }

        @inlinable
        mutating func leaseConnection(at index: Int, streams: UInt16) -> LeaseResult {
            let leaseResult = self.connections[index].lease(streams: streams)
            let use = self.getConnectionUse(index: index)

            if leaseResult.wasIdle {
                self.stats.idle -= 1
                self.stats.leased += 1
            }
            self.stats.leasedStreams += streams
            self.stats.availableStreams -= streams
            return LeaseResult(
                connection: leaseResult.connection,
                timersToCancel: leaseResult.timersToCancel,
                wasIdle: leaseResult.wasIdle,
                use: use
            )
        }

        @inlinable
        mutating func parkConnection(at index: Int) -> Max2Sequence<ConnectionTimer> {
            let scheduleIdleTimeoutTimer: Bool
            switch index {
            case 0..<self.minimumConcurrentConnections:
                // if a connection is a minimum connection, it doesn't need to create an idle
                // timeout timer
                scheduleIdleTimeoutTimer = false

            case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                // if a connection is a demand connection, we want a timeout timer
                scheduleIdleTimeoutTimer = true

            case self.maximumConcurrentConnectionSoftLimit..<self.maximumConcurrentConnectionHardLimit:
                preconditionFailure("Overflow connections should never be parked.")

            default:
                preconditionFailure("A connection index must not be equal or larger `self.maximumConcurrentConnectionHardLimit`")
            }

            return self.connections[index].parkConnection(
                scheduleKeepAliveTimer: self.keepAlive,
                scheduleIdleTimeoutTimer: scheduleIdleTimeoutTimer
            )
        }

        /// A connection was released.
        ///
        /// This will put the position into the idle state.
        ///
        /// - Parameter connectionID: The released connection's id.
        /// - Returns: An index and an IdleConnectionContext to determine the next action for the now idle connection.
        ///            Call ``leaseConnection(at:)`` or ``closeConnection(at:)`` with the supplied index after
        ///            this. If you want to park the connection no further call is required.
        @inlinable
        mutating func releaseConnection(_ connectionID: Connection.ID, streams: UInt16) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            let connectionInfo = self.connections[index].release(streams: streams)
            self.stats.availableStreams += streams
            self.stats.leasedStreams -= streams
            switch connectionInfo {
            case .idle:
                self.stats.idle += 1
                self.stats.leased -= 1
            case .leased:
                break
            }

            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        @inlinable
        mutating func keepAliveIfIdle(_ connectionID: Connection.ID) -> KeepAliveAction? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of ping pong)
                // was already removed from the state machine.
                return nil
            }

            guard let action = self.connections[index].runKeepAliveIfIdle(reducesAvailableStreams: self.keepAliveReducesAvailableStreams) else {
                return nil
            }

            self.stats.runningKeepAlive += 1
            if self.keepAliveReducesAvailableStreams {
                self.stats.availableStreams -= 1
            }

            return action
        }

        @inlinable
        mutating func keepAliveSucceeded(_ connectionID: Connection.ID) -> (Int, AvailableConnectionContext)? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            guard let connectionInfo = self.connections[index].keepAliveSucceeded() else {
                // if we don't get connection info here this means, that the connection already was
                // transitioned to closing. when we did this we already decremented the
                // runningKeepAlive timer.
                return nil
            }

            self.stats.runningKeepAlive -= 1
            if self.keepAliveReducesAvailableStreams {
                self.stats.availableStreams += 1
            }

            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        // MARK: Connection close/removal

        @usableFromInline
        struct CloseAction {
            @usableFromInline
            private(set) var connection: Connection

            @usableFromInline
            private(set) var timersToCancel: Max2Sequence<TimerCancellationToken>

            @inlinable
            init(connection: Connection, timersToCancel: Max2Sequence<TimerCancellationToken>) {
                self.connection = connection
                self.timersToCancel = timersToCancel
            }
        }

        /// Closes the connection at the given index.
        @inlinable
        mutating func closeConnection(at index: Int) -> Connection {
            self.stats.idle -= 1
            self.stats.closing += 1
            fatalError()
//            return self.connections[index].close()
        }

        @inlinable
        mutating func closeConnectionIfIdle(_ connectionID: Connection.ID) -> CloseAction? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of timeout)
                // was already removed from the state machine.
                return nil
            }

            if index < self.minimumConcurrentConnections {
                // because of a race a connection might receive a idle timeout after it was moved into
                // the persisted connections. If a connection is now persisted, we now need to ignore
                // the trigger
                return nil
            }

            guard let closeAction = self.connections[index].closeIfIdle() else {
                return nil
            }

            self.stats.idle -= 1
            self.stats.closing += 1

//            if idleState.runningKeepAlive {
//                self.stats.runningKeepAlive -= 1
//                if self.keepAliveReducesAvailableStreams {
//                    self.stats.availableStreams += 1
//                }
//            }

            self.stats.availableStreams -= closeAction.maxStreams

            return CloseAction(
                connection: closeAction.connection!,
                timersToCancel: closeAction.cancelTimers
            )
        }

        // MARK: Connection failure

        /// Connection closed. Call this method, if a connection is closed.
        ///
        /// This will put the position into the closed state.
        ///
        /// - Parameter connectionID: The failed connection's id.
        /// - Returns: An optional index and an IdleConnectionContext to determine the next action for the closed connection.
        ///            You must call ``removeConnection(at:)`` or ``replaceConnection(at:)`` with the
        ///            supplied index after this. If nil is returned the connection was closed by the state machine and was
        ///            therefore already removed.
        mutating func connectionClosed(_ connectionID: Connection.ID) -> FailedConnectionContext? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                return nil
            }

            let closedAction = self.connections[index].closed()

            if closedAction.wasRunningKeepAlive {
                self.stats.runningKeepAlive -= 1
            }
            self.stats.leasedStreams -= closedAction.usedStreams
            self.stats.availableStreams -= closedAction.maxStreams - closedAction.usedStreams

            switch closedAction.previousConnectionState {
            case .idle:
                self.stats.idle -= 1

            case .leased:
                self.stats.leased -= 1

            case .closing:
                self.stats.closing -= 1
            }

            let lastIndex = self.connections.endIndex - 1

            if index == lastIndex {
                self.connections.remove(at: index)
            } else {
                self.connections.swapAt(index, lastIndex)
                self.connections.remove(at: lastIndex)
            }

            return FailedConnectionContext(connectionsStarting: 0)
        }

        // MARK: Shutdown

        mutating func triggerForceShutdown(_ cleanup: inout ConnectionAction.Shutdown) {
            for var connectionState in self.connections {
                guard let closeAction = connectionState.close() else {
                    continue
                }

                if let connection = closeAction.connection {
                    cleanup.connections.append(connection)
                }
                cleanup.timersToCancel.append(contentsOf: closeAction.cancelTimers)
            }

            self.connections = []
        }

        // MARK: - Private functions -

        @usableFromInline
        /*private*/ func getConnectionUse(index: Int) -> ConnectionUse {
            switch index {
            case 0..<self.minimumConcurrentConnections:
                return .persisted
            case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                return .demand
            case self.maximumConcurrentConnectionSoftLimit...:
                return .overflow
            default:
                preconditionFailure()
            }
        }

        @usableFromInline
        /*private*/ func makeAvailableConnectionContextForConnection(at index: Int, info: ConnectionAvailableInfo) -> AvailableConnectionContext {
            precondition(self.connections[index].isAvailable)
            let use = self.getConnectionUse(index: index)
            return AvailableConnectionContext(use: use, info: info)
        }

        @inlinable
        /*private*/ func findAvailableConnection() -> Int? {
            return self.connections.firstIndex(where: { $0.isAvailable })
        }

        @inlinable
        /*private*/ mutating func swapForDeletion(index indexToDelete: Int) -> TimerCancellationToken? {
            let lastConnectedIndex = self.connections.lastIndex(where: { $0.isConnected })

            if lastConnectedIndex == nil || lastConnectedIndex! < indexToDelete {
                self.removeO1(indexToDelete)
                return nil
            }

            guard let lastConnectedIndex = lastConnectedIndex else { preconditionFailure() }

            switch indexToDelete {
            case 0..<self.minimumConcurrentConnections:
                // the connection to be removed is a persisted connection
                self.connections.swapAt(indexToDelete, lastConnectedIndex)
                self.removeO1(lastConnectedIndex)

                switch lastConnectedIndex {
                case 0..<self.minimumConcurrentConnections:
                    // a persisted connection was moved within the persisted connections. thats fine.
                    return nil

                case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                    // a demand connection was moved to a persisted connection. If it currently idle
                    // or ping ponging, we must cancel its idle timeout timer
                    return self.connections[indexToDelete].cancelIdleTimer()

                case self.maximumConcurrentConnectionSoftLimit..<self.maximumConcurrentConnectionHardLimit:
                    // an overflow connection was moved to a demand connection. It has to be currently leased
                    precondition(self.connections[indexToDelete].isLeased)
                    return nil

                default:
                    preconditionFailure("A connection index must not be equal or larger `self.maximumConcurrentConnectionHardLimit`")
                }

            case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                // the connection to be removed is a demand connection
                switch lastConnectedIndex {
                case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                    // an overflow connection was moved to a demand connection. It has to be currently leased
                    precondition(self.connections[indexToDelete].isLeased)
                    return nil

                default:
                    return nil
                }

            default:
                return nil
            }
        }

        @inlinable
        /*private*/ mutating func removeO1(_ indexToDelete: Int) {
            let lastIndex = self.connections.endIndex - 1

            if indexToDelete == lastIndex {
                self.connections.remove(at: indexToDelete)
            } else {
                self.connections.swapAt(indexToDelete, lastIndex)
                self.connections.remove(at: lastIndex)
            }
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension PoolStateMachine.ConnectionGroup.BackoffDoneAction: Equatable where TimerCancellationToken: Equatable {}
