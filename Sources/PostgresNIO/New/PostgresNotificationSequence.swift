
public struct PostgresNotificationSequence: AsyncSequence {
    public typealias Element = String

    let base: AsyncThrowingStream<String, Error>

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: self.base.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<String, Error>.AsyncIterator

        public mutating func next() async throws -> Element? {
            try await self.base.next()
        }
    }
}
