
public struct ConnectionPoolError<Underlying: Error>: Error {

    public struct Code: Sendable, Hashable {
        @usableFromInline
        enum Base: Error, Hashable, Sendable {
            case requestCancelled
            case poolShutdown
            case connectionCreationCircuitBreakerTripped
        }

        @usableFromInline
        let base: Base

        @inlinable
        init(_ base: Base) {
            self.base = base
        }

        /// The connection requests got cancelled
        @inlinable
        public static var requestCancelled: Self {
            Code(.requestCancelled)
        }
        /// The connection requests can't be fulfilled as the pool has already been shutdown
        @inlinable
        public static var poolShutdown: Self {
            Code(.poolShutdown)
        }
        /// The connection pool has failed to make a connection after a defined time
        @inlinable
        public static var connectionCreationCircuitBreakerTripped: Self {
            Code(.connectionCreationCircuitBreakerTripped)
        }
    }

    public let code: Code

    public let underlying: Underlying?

    @inlinable
    init(_ code: Code, underlying: Underlying? = nil) {
        self.code = code
        self.underlying = underlying
    }
}

extension ConnectionPoolError: Equatable where Underlying: Equatable {}
extension ConnectionPoolError: Hashable where Underlying: Hashable {}
extension ConnectionPoolError: Sendable where Underlying: Sendable {}
