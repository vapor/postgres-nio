@testable import _ConnectionPoolModule

struct MockTimerCancellationToken: Hashable, Sendable {
    var connectionID: MockConnection.ID
    var timerID: Int
    var duration: Duration
    var usecase: TestPoolStateMachine.Timer.Usecase

    init(_ timer: TestPoolStateMachine.Timer) {
        self.connectionID = timer.connectionID
        self.timerID = timer.timerID
        self.duration = timer.duration
        self.usecase = timer.usecase
    }
}
