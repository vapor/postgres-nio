import _ConnectionPoolModule
import DequeModule
import NIOConcurrencyHelpers

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MockConnectionFactory<Clock: _Concurrency.Clock, Executor: ConnectionPoolExecutor>: Sendable where Clock.Duration == Duration {
    public typealias ConnectionIDGenerator = _ConnectionPoolModule.ConnectionIDGenerator
    public typealias Request = ConnectionRequest<MockConnection<Executor>>
    public typealias KeepAliveBehavior = MockPingPongBehavior
    public typealias MetricsDelegate = NoOpConnectionPoolMetrics<Int>
    public typealias ConnectionID = Int
    public typealias Connection = MockConnection

    @usableFromInline
    let stateBox = NIOLockedValueBox(State())

    @usableFromInline
    struct State {
        @usableFromInline
        var attempts = Deque<(ConnectionID, Executor, CheckedContinuation<(MockConnection<Executor>, UInt16), any Error>)>()

        @usableFromInline
        var waiter = Deque<CheckedContinuation<(ConnectionID, Executor, CheckedContinuation<(MockConnection<Executor>, UInt16), any Error>), Never>>()

        @usableFromInline
        var runningConnections = [ConnectionID: Connection<Executor>]()
    }

    @usableFromInline
    let autoMaxStreams: UInt16?

    public init(autoMaxStreams: UInt16? = nil) {
        self.autoMaxStreams = autoMaxStreams
    }

    public var pendingConnectionAttemptsCount: Int {
        self.stateBox.withLockedValue { $0.attempts.count }
    }

    public var runningConnections: [Connection<Executor>] {
        self.stateBox.withLockedValue { Array($0.runningConnections.values) }
    }

    @inlinable
    public func makeConnection(
        id: Int,
        configuration: MockConnectionConfiguration,
        for pool: ConnectionPool<MockConnection<Executor>, Int, ConnectionIDGenerator, MockConnectionConfiguration, some ConnectionRequestProtocol, Int, MockPingPongBehavior<MockConnection<Executor>>, Executor, NoOpConnectionPoolMetrics<Int>, Clock>
    ) async throws -> ConnectionAndMetadata<MockConnection<Executor>> {
        if let autoMaxStreams = self.autoMaxStreams {
            let connection = MockConnection(id: id, executor: pool.executor)
            Task {
                try? await connection.signalToClose
                connection.closeIfClosing()
            }
            return .init(connection: connection, maximalStreamsOnConnection: autoMaxStreams)
        }

        // we currently don't support cancellation when creating a connection
        let result = try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<(MockConnection<Executor>, UInt16), any Error>) in
            let waiter = self.stateBox.withLockedValue { state -> (CheckedContinuation<(ConnectionID, Executor, CheckedContinuation<(MockConnection<Executor>, UInt16), any Error>), Never>)? in
                if let waiter = state.waiter.popFirst() {
                    return waiter
                } else {
                    state.attempts.append((id, pool.executor, checkedContinuation))
                    return nil
                }
            }

            if let waiter {
                waiter.resume(returning: (id, pool.executor, checkedContinuation))
            }
        }

        return .init(connection: result.0, maximalStreamsOnConnection: result.1)
    }

    @discardableResult
    @inlinable
    public func nextConnectAttempt(_ closure: (ConnectionID) async throws -> UInt16) async rethrows -> Connection<Executor> {
        let (connectionID, executor, continuation) = await withCheckedContinuation { (continuation: CheckedContinuation<(ConnectionID, Executor, CheckedContinuation<(MockConnection, UInt16), any Error>), Never>) in
            let attempt = self.stateBox.withLockedValue { state -> (ConnectionID, Executor, CheckedContinuation<(MockConnection<Executor>, UInt16), any Error>)? in
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
            let connection = MockConnection(id: connectionID, executor: executor)

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
