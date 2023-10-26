@testable import _ConnectionPoolModule
import Atomics

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class MockClock: Clock {
    struct Instant: InstantProtocol, Comparable {
        typealias Duration = Swift.Duration

        func advanced(by duration: Self.Duration) -> Self {
            .init(self.base + duration)
        }

        func duration(to other: Self) -> Self.Duration {
            self.base - other.base
        }

        private var base: Swift.Duration

        init(_ base: Duration) {
            self.base = base
        }

        static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.base < rhs.base
        }

        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.base == rhs.base
        }
    }

    private struct State: Sendable {
        var now: Instant

        var sleepersHeap: Array<Sleeper>

        var waitersHeap: Array<Waiter>

        init() {
            self.now = .init(.seconds(0))
            self.sleepersHeap = Array()
            self.waitersHeap = Array()
        }
    }

    private struct Waiter {
        var expectedSleepers: Int

        var continuation: CheckedContinuation<Void, Never>
    }

    private struct Sleeper {
        var id: Int

        var deadline: Instant

        var continuation: CheckedContinuation<Void, any Error>
    }

    typealias Duration = Swift.Duration

    var minimumResolution: Duration { .nanoseconds(1) }

    var now: Instant { self.stateBox.withLockedValue { $0.now } }

    private let stateBox = NIOLockedValueBox(State())
    private let waiterIDGenerator = ManagedAtomic(0)

    func sleep(until deadline: Instant, tolerance: Duration?) async throws {
        let waiterID = self.waiterIDGenerator.loadThenWrappingIncrement(ordering: .relaxed)

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                enum SleepAction {
                    case none
                    case resume
                    case cancel
                }

                let action = self.stateBox.withLockedValue { state -> (SleepAction, ArraySlice<Waiter>) in
                    state.waitersHeap = state.waitersHeap.map { waiter in
                        var waiter = waiter; waiter.expectedSleepers -= 1; return waiter
                    }
                    let slice: ArraySlice<Waiter>
                    let lastRemainingIndex = state.waitersHeap.firstIndex(where: { $0.expectedSleepers > 0 })
                    if let lastRemainingIndex {
                        slice = state.waitersHeap[0..<lastRemainingIndex]
                        state.waitersHeap.removeFirst(lastRemainingIndex)
                    } else if !state.waitersHeap.isEmpty {
                        slice = state.waitersHeap[...]
                        state.waitersHeap.removeAll()
                    } else {
                        slice = []
                    }

                    if Task.isCancelled {
                        return (.cancel, slice)
                    }

                    if state.now >= deadline {
                        return (.resume, slice)
                    }

                    let newWaiter = Sleeper(id: waiterID, deadline: deadline, continuation: continuation)

                    if let index = state.sleepersHeap.lastIndex(where: { $0.deadline < deadline }) {
                        state.sleepersHeap.insert(newWaiter, at: index + 1)
                    } else {
                        state.sleepersHeap.append(newWaiter)
                    }

                    return (.none, slice)
                }

                switch action.0 {
                case .cancel:
                    continuation.resume(throwing: CancellationError())
                case .resume:
                    continuation.resume()
                case .none:
                    break
                }

                for waiter in action.1 {
                    waiter.continuation.resume()
                }
            }
        } onCancel: {
            let continuation = self.stateBox.withLockedValue { state -> CheckedContinuation<Void, any Error>? in
                if let index = state.sleepersHeap.firstIndex(where: { $0.id == waiterID }) {
                    return state.sleepersHeap.remove(at: index).continuation
                }
                return nil
            }
            continuation?.resume(throwing: CancellationError())
        }
    }

    func timerScheduled(n: Int = 1) async {
        precondition(n >= 1, "At least one new sleep must be awaited")
        await withCheckedContinuation { (continuation: CheckedContinuation<(), Never>) in
            let result = self.stateBox.withLockedValue { state -> Bool in
                let n = n - state.sleepersHeap.count

                if n <= 0 {
                    return true
                }

                let waiter = Waiter(expectedSleepers: n, continuation: continuation)

                if let index = state.waitersHeap.firstIndex(where: { $0.expectedSleepers > n }) {
                    state.waitersHeap.insert(waiter, at: index)
                } else {
                    state.waitersHeap.append(waiter)
                }
                return false
            }

            if result {
                continuation.resume()
            }
        }
    }

    func advance(to deadline: Instant) {
        let waiters = self.stateBox.withLockedValue { state -> ArraySlice<Sleeper> in
            precondition(deadline > state.now, "Time can only move forward")
            state.now = deadline

            if let newFirstIndex = state.sleepersHeap.firstIndex(where: { $0.deadline > deadline }) {
                defer { state.sleepersHeap.removeFirst(newFirstIndex) }
                return state.sleepersHeap[0..<newFirstIndex]
            } else if let first = state.sleepersHeap.first, first.deadline <= deadline {
                defer { state.sleepersHeap = [] }
                return state.sleepersHeap[...]
            } else {
                return []
            }
        }

        for waiter in waiters {
            waiter.continuation.resume()
        }
    }
}

