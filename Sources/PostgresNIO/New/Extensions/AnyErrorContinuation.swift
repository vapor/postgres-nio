/// Any `CheckedContinuation` that has an error type of `any Error`.
protocol AnyErrorContinuation {
    func resume(throwing error: any Error)
}

extension CheckedContinuation: AnyErrorContinuation where E == any Error {}
