import Atomics
@testable import _ConnectionPoolModule

final class Waiter<Result: Sendable>: Sendable {
    struct State: Sendable {

        var result: Swift.Result<Result, any Error>? = nil
        var continuations: [(Int, CheckedContinuation<Result, any Error>)] = []

    }

    let waiterID = ManagedAtomic(0)
    let stateBox: NIOLockedValueBox<State> = NIOLockedValueBox(State())

    init(of: Result.Type) {}

    enum GetAction {
        case fail(any Error)
        case succeed(Result)
        case none
    }

    var result: Result {
        get async throws {
            let waiterID = self.waiterID.loadThenWrappingIncrement(ordering: .relaxed)

            return try await withTaskCancellationHandler {
                return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Result, any Error>) in
                    let action = self.stateBox.withLockedValue { state -> GetAction in
                        if Task.isCancelled {
                            return .fail(CancellationError())
                        }

                        switch state.result {
                        case .none:
                            state.continuations.append((waiterID, continuation))
                            return .none

                        case .success(let result):
                            return .succeed(result)

                        case .failure(let error):
                            return .fail(error)
                        }
                    }

                    switch action {
                    case .fail(let error):
                        continuation.resume(throwing: error)

                    case .succeed(let result):
                        continuation.resume(returning: result)

                    case .none:
                        break
                    }
                }
            } onCancel: {
                let cont = self.stateBox.withLockedValue { state -> CheckedContinuation<Result, any Error>? in
                    guard state.result == nil else { return nil }

                    guard let contIndex = state.continuations.firstIndex(where: { $0.0 == waiterID }) else {
                        return nil
                    }
                    let (_, continuation) = state.continuations.remove(at: contIndex)
                    return continuation
                }

                cont?.resume(throwing: CancellationError())
            }
        }
    }

    func yield(value: Result) {
        let continuations = self.stateBox.withLockedValue { state in
            guard state.result == nil else {
                return [(Int, CheckedContinuation<Result, any Error>)]().lazy.map(\.1)
            }
            state.result = .success(value)

            let continuations = state.continuations
            state.continuations = []

            return continuations.lazy.map(\.1)
        }

        for continuation in continuations {
            continuation.resume(returning: value)
        }
    }

    func yield(error: any Error) {
        let continuations = self.stateBox.withLockedValue { state in
            guard state.result == nil else {
                return [(Int, CheckedContinuation<Result, any Error>)]().lazy.map(\.1)
            }
            state.result = .failure(error)

            let continuations = state.continuations
            state.continuations = []

            return continuations.lazy.map(\.1)
        }

        for continuation in continuations {
            continuation.resume(throwing: error)
        }
    }
}
