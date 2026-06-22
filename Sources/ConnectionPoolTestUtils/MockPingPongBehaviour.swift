import _ConnectionPoolModule
import DequeModule
import NIOConcurrencyHelpers

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MockPingPongBehavior<Connection: PooledConnection>: ConnectionKeepAliveBehavior {
    public let keepAliveFrequency: Duration?

    let stateBox = NIOLockedValueBox(State())

    struct State {
        var runs = Deque<(Connection, CheckedContinuation<Bool, any Error>)>()

        var waiter = Deque<CheckedContinuation<(Connection, CheckedContinuation<Bool, any Error>), Never>>()
    }

    public init(keepAliveFrequency: Duration?, connectionType: Connection.Type) {
        self.keepAliveFrequency = keepAliveFrequency
    }

    public func runKeepAlive(for connection: Connection) async throws {
        precondition(self.keepAliveFrequency != nil)

        // we currently don't support cancellation when creating a connection
        let success = try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<Bool, any Error>) -> () in
            let waiter = self.stateBox.withLockedValue { state -> (CheckedContinuation<(Connection, CheckedContinuation<Bool, any Error>), Never>)? in
                if let waiter = state.waiter.popFirst() {
                    return waiter
                } else {
                    state.runs.append((connection, checkedContinuation))
                    return nil
                }
            }

            if let waiter {
                waiter.resume(returning: (connection, checkedContinuation))
            }
        }

        precondition(success)
    }

    @discardableResult
    public func nextKeepAlive(_ closure: (Connection) async throws -> Bool) async rethrows -> Connection {
        let (connection, continuation) = await withCheckedContinuation { (continuation: CheckedContinuation<(Connection, CheckedContinuation<Bool, any Error>), Never>) in
            let run = self.stateBox.withLockedValue { state -> (Connection, CheckedContinuation<Bool, any Error>)? in
                if let run = state.runs.popFirst() {
                    return run
                } else {
                    state.waiter.append(continuation)
                    return nil
                }
            }

            if let run {
                continuation.resume(returning: run)
            }
        }

        do {
            let success = try await closure(connection)

            continuation.resume(returning: success)
            return connection
        } catch {
            continuation.resume(throwing: error)
            throw error
        }
    }
}
