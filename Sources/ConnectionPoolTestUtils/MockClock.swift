import _ConnectionPoolModule
import Atomics
import DequeModule
import NIOConcurrencyHelpers

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MockClock: Clock {
    public struct Instant: InstantProtocol, Comparable {
        public typealias Duration = Swift.Duration

        public func advanced(by duration: Self.Duration) -> Self {
            .init(self.base + duration)
        }

        public func duration(to other: Self) -> Self.Duration {
            other.base - self.base
        }

        private var base: Swift.Duration

        public init(_ base: Duration) {
            self.base = base
        }

        public static func < (lhs: Self, rhs: Self) -> Bool {
            lhs.base < rhs.base
        }

        public static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.base == rhs.base
        }
    }

    private struct State: Sendable {
        var now: Instant

        var sleepersHeap: Array<Sleeper>

        var waiters: Deque<Waiter>
        var nextDeadlines: Deque<Instant>

        init() {
            self.now = .init(.seconds(0))
            self.sleepersHeap = Array()
            self.waiters = Deque()
            self.nextDeadlines = Deque()
        }
    }

    private struct Waiter {
        var continuation: CheckedContinuation<Instant, Never>
    }

    private struct Sleeper {
        var id: Int

        var deadline: Instant

        var continuation: CheckedContinuation<Void, any Error>
    }

    public typealias Duration = Swift.Duration

    public var minimumResolution: Duration { .nanoseconds(1) }

    public var now: Instant { self.stateBox.withLockedValue { $0.now } }

    private let stateBox = NIOLockedValueBox(State())
    private let waiterIDGenerator = ManagedAtomic(0)

    public init() {}

    public func sleep(until deadline: Instant, tolerance: Duration?) async throws {
        let waiterID = self.waiterIDGenerator.loadThenWrappingIncrement(ordering: .relaxed)

        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                enum SleepAction {
                    case none
                    case resume
                    case cancel
                }

                let action = self.stateBox.withLockedValue { state -> (SleepAction, Waiter?) in
                    let waiter: Waiter?
                    if let next = state.waiters.popFirst() {
                        waiter = next
                    } else {
                        state.nextDeadlines.append(deadline)
                        waiter = nil
                    }

                    if Task.isCancelled {
                        return (.cancel, waiter)
                    }

                    if state.now >= deadline {
                        return (.resume, waiter)
                    }

                    let newSleeper = Sleeper(id: waiterID, deadline: deadline, continuation: continuation)

                    if let index = state.sleepersHeap.lastIndex(where: { $0.deadline < deadline }) {
                        state.sleepersHeap.insert(newSleeper, at: index + 1)
                    } else if let first = state.sleepersHeap.first, first.deadline > deadline {
                        state.sleepersHeap.insert(newSleeper, at: 0)
                    } else {
                        state.sleepersHeap.append(newSleeper)
                    }

                    return (.none, waiter)
                }

                switch action.0 {
                case .cancel:
                    continuation.resume(throwing: CancellationError())
                case .resume:
                    continuation.resume()
                case .none:
                    break
                }

                action.1?.continuation.resume(returning: deadline)
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

    @discardableResult
    public func nextTimerScheduled() async -> Instant {
        await withCheckedContinuation { (continuation: CheckedContinuation<Instant, Never>) in
            let instant = self.stateBox.withLockedValue { state -> Instant? in
                if let scheduled = state.nextDeadlines.popFirst() {
                    return scheduled
                } else {
                    let waiter = Waiter(continuation: continuation)
                    state.waiters.append(waiter)
                    return nil
                }
            }

            if let instant {
                continuation.resume(returning: instant)
            }
        }
    }

    public func advance(to deadline: Instant) {
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

