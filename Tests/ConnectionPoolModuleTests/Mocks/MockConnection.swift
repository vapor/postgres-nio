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

