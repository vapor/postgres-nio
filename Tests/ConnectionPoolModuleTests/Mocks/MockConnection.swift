import DequeModule
@testable import _ConnectionPoolModule

// Sendability enforced through the lock
final class MockConnection: PooledConnection, Sendable {
    typealias ID = Int

    let id: ID

    private enum State {
        case running([@Sendable ((any Error)?) -> ()])
        case closing([@Sendable ((any Error)?) -> ()])
        case closed
    }

    private let lock: NIOLockedValueBox<State> = NIOLockedValueBox(.running([]))

    init(id: Int) {
        self.id = id
    }

    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ()) {
        let enqueued = self.lock.withLockedValue { state -> Bool in
            switch state {
            case .closed:
                return false

            case .running(var callbacks):
                callbacks.append(closure)
                state = .running(callbacks)
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

    func close() {
        self.lock.withLockedValue { state in
            switch state {
            case .running(let callbacks):
                state = .closing(callbacks)

            case .closing, .closed:
                break
            }
        }
    }

    func closeIfClosing() {
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
    var description: String {
        let state = self.lock.withLockedValue { $0 }
        return "MockConnection(id: \(self.id), state: \(state))"
    }
}
