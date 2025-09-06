
public struct PostgresNotification: Sendable {
    public let payload: String
}

public struct PostgresNotificationSequence: AsyncSequence, Sendable {
    public typealias Element = PostgresNotification

    let base: AsyncThrowingStream<PostgresNotification, any Error>

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: self.base.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<PostgresNotification, any Error>.AsyncIterator

        public mutating func next() async throws -> Element? {
            try await self.base.next()
        }
    }
}

@available(*, unavailable)
extension PostgresNotificationSequence.AsyncIterator: Sendable {}
