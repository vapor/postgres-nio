import _ConnectionPoolModule
import DequeModule
import NIOConcurrencyHelpers

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MockConnectionFactory<Clock: _Concurrency.Clock>: Sendable where Clock.Duration == Duration {
    public typealias ConnectionIDGenerator = _ConnectionPoolModule.ConnectionIDGenerator
    public typealias Request = MockRequest<MockConnection>
    public typealias KeepAliveBehavior = MockPingPongBehavior
    public typealias MetricsDelegate = NoOpConnectionPoolMetrics<Int>
    public typealias ConnectionID = Int
    public typealias Connection = MockConnection

    let stateBox = NIOLockedValueBox(State())

    struct State {
        var attempts = Deque<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>)>()

        var waiter = Deque<CheckedContinuation<(ConnectionID, CheckedContinuation<(MockConnection, UInt16), any Error>), Never>>()

        var runningConnections = [ConnectionID: Connection]()
    }

    let autoMaxStreams: UInt16?

    public init(autoMaxStreams: UInt16? = nil) {
        self.autoMaxStreams = autoMaxStreams
    }

    public var pendingConnectionAttemptsCount: Int {
        self.stateBox.withLockedValue { $0.attempts.count }
    }

    public var runningConnections: [Connection] {
        self.stateBox.withLockedValue { Array($0.runningConnections.values) }
    }

    public func makeConnection(
        id: Int,
        for pool: ConnectionPool<MockConnection, Int, ConnectionIDGenerator, MockError, Request, MockRequest<MockConnection>.ID, MockPingPongBehavior<MockConnection>, NoOpConnectionPoolMetrics<Int>, Clock, Clock.Instant>
    ) async throws -> ConnectionAndMetadata<MockConnection> {
        if let autoMaxStreams = self.autoMaxStreams {
            let connection = MockConnection(id: id)
            Task {
                try? await connection.signalToClose
                connection.closeIfClosing()
            }
            return .init(connection: connection, maximalStreamsOnConnection: autoMaxStreams)
        }

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
    public func nextConnectAttempt(_ closure: (ConnectionID) async throws(MockError) -> UInt16) async rethrows -> Connection {
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
