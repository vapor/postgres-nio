import DequeModule
@testable import _ConnectionPoolModule

// Sendability enforced through the lock
final class MockConnection: PooledConnection, @unchecked Sendable {
    typealias ID = Int

    let id: ID

    private enum State {
        case running([@Sendable ((any Error)?) -> ()])
        case closing([@Sendable ((any Error)?) -> ()])
        case closed
    }

    private let lock = NIOLock()
    private var _state = State.running([])

    init(id: Int) {
        self.id = id
    }

    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ()) {
        let enqueued = self.lock.withLock { () -> Bool in
            switch self._state {
            case .closed:
                return false

            case .running(var callbacks):
                callbacks.append(closure)
                self._state = .running(callbacks)
                return true

            case .closing(var callbacks):
                callbacks.append(closure)
                self._state = .closing(callbacks)
                return true
            }
        }

        if !enqueued {
            closure(nil)
        }
    }

    func close() {
        self.lock.withLock {
            switch self._state {
            case .running(let callbacks):
                self._state = .closing(callbacks)

            case .closing, .closed:
                break
            }
        }
    }

    func closeIfClosing() {
        let callbacks = self.lock.withLock { () -> [@Sendable ((any Error)?) -> ()] in
            switch self._state {
            case .running, .closed:
                return []

            case .closing(let callbacks):
                self._state = .closed
                return callbacks
            }
        }

        for callback in callbacks {
            callback(nil)
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class MockConnectionFactory<Clock: _Concurrency.Clock> where Clock.Duration == Duration {
    typealias ConnectionIDGenerator = _ConnectionPoolModule.ConnectionIDGenerator
    typealias Request = ConnectionRequest<MockConnection>
    typealias KeepAliveBehavior = MockPingPongBehavior
    typealias MetricsDelegate = NoOpConnectionPoolMetrics<Int>
    typealias ConnectionID = Int
    typealias Connection = MockConnection

    let stateBox = NIOLockedValueBox(State())

    struct State {
        var attempts = Deque<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>)>()

        var waiter = Deque<CheckedContinuation<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>), Never>>()
    }

    var pendingConnectionAttemptsCount: Int {
        self.stateBox.withLockedValue { $0.attempts.count }
    }

    func makeConnection(
        id: Int,
        for pool: ConnectionPool<MockConnection, Int, ConnectionIDGenerator, ConnectionRequest<MockConnection>, Int, MockPingPongBehavior, NoOpConnectionPoolMetrics<Int>, Clock>
    ) async throws -> ConnectionAndMetadata<MockConnection> {
        // we currently don't support cancellation when creating a connection
        let result = try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<(MockConnection, UInt16), any Error>) in
            let waiter = self.stateBox.withLockedValue { state -> (CheckedContinuation<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>), Never>)? in
                if let waiter = state.waiter.popFirst() {
                    return waiter
                } else {
                    state.attempts.append((id, checkedContinuation))
                    return nil
                }
            }

            if let waiter {
                waiter.resume(returning: (id, checkedContinuation))
            }
        }

        return .init(connection: result.0, maximalStreamsOnConnection: result.1)
    }

    @discardableResult
    func nextConnectAttempt(_ closure: (ConnectionID) async throws -> UInt16) async rethrows -> Connection {
        let (connectionID, continuation) = await withCheckedContinuation { (continuation: CheckedContinuation<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>), Never>) in
            let attempt = self.stateBox.withLockedValue { state -> (ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>)? in
                if let attempt = state.attempts.popFirst() {
                    return attempt
                } else {
                    state.waiter.append(continuation)
                    return nil
                }
            }

            if let attempt {
                continuation.resume(returning: attempt)
            }
        }

        do {
            let streamCount = try await closure(connectionID)
            let connection = MockConnection(id: connectionID)
            continuation.resume(returning: (connection, streamCount))
            return connection
        } catch {
            continuation.resume(throwing: error)
            throw error
        }
    }
}
