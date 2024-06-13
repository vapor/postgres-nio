@testable import _ConnectionPoolModule
import DequeModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class MockConnectionFactory<Clock: _Concurrency.Clock>: Sendable where Clock.Duration == Duration {
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

        var runningConnections = [ConnectionID: Connection]()
    }

    var pendingConnectionAttemptsCount: Int {
        self.stateBox.withLockedValue { $0.attempts.count }
    }

    var runningConnections: [Connection] {
        self.stateBox.withLockedValue { Array($0.runningConnections.values) }
    }

    func makeConnection(
        id: Int,
        for pool: ConnectionPool<MockConnection, Int, ConnectionIDGenerator, some ConnectionRequestProtocol, Int, MockPingPongBehavior<MockConnection>, NoOpConnectionPoolMetrics<Int>, Clock>
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

            connection.onClose { _ in
                self.stateBox.withLockedValue { state in
                    _ = state.runningConnections.removeValue(forKey: connectionID)
                }
            }

            self.stateBox.withLockedValue { state in
                _ = state.runningConnections[connectionID] = connection
            }

            continuation.resume(returning: (connection, streamCount))
            return connection
        } catch {
            continuation.resume(throwing: error)
            throw error
        }
    }
}
