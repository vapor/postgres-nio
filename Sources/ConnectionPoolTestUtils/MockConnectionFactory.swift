import _ConnectionPoolModule
import Atomics
import DequeModule
import NIOConcurrencyHelpers

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MockConnectionFactory<Clock: _Concurrency.Clock>: ConnectionProvider, Sendable where Clock.Duration == Duration {
    typealias ConnectionIDGenerator = _ConnectionPoolModule.ConnectionIDGenerator
    public typealias Request = ConnectionRequest<MockConnection>
    public typealias KeepAliveBehavior = MockPingPongBehavior
    public typealias ConnectionID = Int
    public typealias Connection = MockConnection

    let stateBox = NIOLockedValueBox(State())
    let mockIDGenerator = ManagedAtomic<Int>(0)

    struct State {
        var attempts = Deque<CheckedContinuation<(MockConnection, UInt16), any Error>>()

        var waiter = Deque<CheckedContinuation<CheckedContinuation<(MockConnection, UInt16), any Error>, Never>>()

        var runningConnections = [Int: Connection]()
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

    public func withConnection(
        onConnected: (consuming MockConnection, UInt16, (EventsCallbacks) -> Void) async -> Void
    ) async throws {
        if let autoMaxStreams = self.autoMaxStreams {
            let id = self.mockIDGenerator.wrappingIncrementThenLoad(ordering: .relaxed)
            let connection = MockConnection(id: id)

            self.stateBox.withLockedValue { state in
                state.runningConnections[id] = connection
            }

            await onConnected(connection, autoMaxStreams) { callbacks in
                connection.onClose { error in
                    callbacks.connectionClosed(error)
                }
            }

            // After onConnected returns, close the connection (structured lifecycle)
            connection.close()
            connection.closeIfClosing()

            self.stateBox.withLockedValue { state in
                _ = state.runningConnections.removeValue(forKey: id)
            }
            return
        }

        // Manual mode: coordinate with nextConnectAttempt
        let (connection, maxStreams) = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<(MockConnection, UInt16), any Error>) in
            let waiter = self.stateBox.withLockedValue { state -> CheckedContinuation<CheckedContinuation<(MockConnection, UInt16), any Error>, Never>? in
                if let waiter = state.waiter.popFirst() {
                    return waiter
                } else {
                    state.attempts.append(continuation)
                    return nil
                }
            }

            if let waiter {
                waiter.resume(returning: continuation)
            }
        }

        await onConnected(connection, maxStreams) { callbacks in
            connection.onClose { error in
                callbacks.connectionClosed(error)
            }
        }

        // After onConnected returns, close the connection (structured lifecycle)
        connection.close()
        connection.closeIfClosing()

        self.stateBox.withLockedValue { state in
            _ = state.runningConnections.removeValue(forKey: connection.id)
        }
    }

    @discardableResult
    public func nextConnectAttempt(_ closure: (ConnectionID) async throws -> UInt16) async rethrows -> Connection {
        let continuation = await withCheckedContinuation { (outerContinuation: CheckedContinuation<CheckedContinuation<(MockConnection, UInt16), any Error>, Never>) in
            let attempt = self.stateBox.withLockedValue { state -> CheckedContinuation<(MockConnection, UInt16), any Error>? in
                if let attempt = state.attempts.popFirst() {
                    return attempt
                } else {
                    state.waiter.append(outerContinuation)
                    return nil
                }
            }

            if let attempt {
                outerContinuation.resume(returning: attempt)
            }
        }

        let mockID = self.mockIDGenerator.wrappingIncrementThenLoad(ordering: .relaxed)

        do {
            let streamCount = try await closure(mockID)
            let connection = MockConnection(id: mockID)

            self.stateBox.withLockedValue { state in
                state.runningConnections[mockID] = connection
            }

            continuation.resume(returning: (connection, streamCount))
            return connection
        } catch {
            continuation.resume(throwing: error)
            throw error
        }
    }
}
