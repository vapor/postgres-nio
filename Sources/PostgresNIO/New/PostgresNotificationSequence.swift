
public struct PostgresNotification: Sendable {
    public let payload: String
}

public struct PostgresNotificationSequence: AsyncSequence {
    public typealias Element = PostgresNotification

    let base: AsyncThrowingStream<PostgresNotification, Error>

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: self.base.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<PostgresNotification, Error>.AsyncIterator

        public mutating func next() async throws -> Element? {
            try await self.base.next()
        }
    }
}

#if swift(>=5.7)
// AsyncThrowingStream is marked as Sendable in Swift 5.6
extension PostgresNotificationSequence: Sendable {}
#endif
