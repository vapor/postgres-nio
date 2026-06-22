@testable import _ConnectionPoolModule

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct MockTimerCancellationToken: Hashable, Sendable {
    enum Backing: Hashable, Sendable {
        case timer(TestPoolStateMachine.Timer)
        case connectionTimer(TestPoolStateMachine.ConnectionTimer)
    }
    var backing: Backing

    init(_ timer: TestPoolStateMachine.Timer) {
        self.backing = .timer(timer)
    }

    init(_ timer: TestPoolStateMachine.ConnectionTimer) {
        self.backing = .connectionTimer(timer)
    }
}
