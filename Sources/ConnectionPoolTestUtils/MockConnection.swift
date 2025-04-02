import _ConnectionPoolModule
import DequeModule
import NIOConcurrencyHelpers

public final class MockConnection: PooledConnection, Sendable {
    public typealias ID = Int

    public let id: ID

    private enum State {
        case running([CheckedContinuation<Void, any Error>], [@Sendable ((any Error)?) -> ()])
        case closing([@Sendable ((any Error)?) -> ()])
        case closed
    }

    private let lock: NIOLockedValueBox<State> = NIOLockedValueBox(.running([], []))

    public init(id: Int) {
        self.id = id
    }

    public var signalToClose: Void {
        get async throws {
            try await withCheckedThrowingContinuation { continuation in
                let runRightAway = self.lock.withLockedValue { state -> Bool in
                    switch state {
                    case .running(var continuations, let callbacks):
                        continuations.append(continuation)
                        state = .running(continuations, callbacks)
                        return false

                    case .closing, .closed:
                        return true
                    }
                }

                if runRightAway {
                    continuation.resume()
                }
            }
        }
    }

    public func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ()) {
        let enqueued = self.lock.withLockedValue { state -> Bool in
            switch state {
            case .closed:
                return false

            case .running(let continuations, var callbacks):
                callbacks.append(closure)
                state = .running(continuations, callbacks)
                return true

            case .closing(var callbacks):
                callbacks.append(closure)
                state = .closing(callbacks)
                return true
            }
        }

        if !enqueued {
            closure(nil)
        }
    }

    public func close() {
        let continuations = self.lock.withLockedValue { state -> [CheckedContinuation<Void, any Error>] in
            switch state {
            case .running(let continuations, let callbacks):
                state = .closing(callbacks)
                return continuations

            case .closing, .closed:
                return []
            }
        }

        for continuation in continuations {
            continuation.resume()
        }
    }

    public func closeIfClosing() {
        let callbacks = self.lock.withLockedValue { state -> [@Sendable ((any Error)?) -> ()] in
            switch state {
            case .running, .closed:
                return []

            case .closing(let callbacks):
                state = .closed
                return callbacks
            }
        }

        for callback in callbacks {
            callback(nil)
        }
    }
}

extension MockConnection: CustomStringConvertible {
    public var description: String {
        let state = self.lock.withLockedValue { $0 }
        return "MockConnection(id: \(self.id), state: \(state))"
    }
}
