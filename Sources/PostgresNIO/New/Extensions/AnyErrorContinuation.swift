/// Enum that abstracts over continuations that have `any Error` as the failure type. Cases are expected to get added
/// for the success types that we care about.
enum AnyErrorContinuation {
    case void(CheckedContinuation<Void, any Error>)
    case copyFromWriter(CheckedContinuation<PostgresCopyFromWriter, any Error>)

    func resume(throwing error: any Error) {
        switch self {
        case .void(let continuation): continuation.resume(throwing: error)
        case .copyFromWriter(let continuation): continuation.resume(throwing: error)
        }
    }
}
