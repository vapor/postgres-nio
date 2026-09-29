
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

        @concurrent
        public mutating func next() async throws -> Element? {
            try await self.base.next()
        }

        @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
        public mutating func next(isolation actor: isolated (any Actor)?) async throws(Self.Failure) -> Element? {
            try await self.base.next(isolation: actor)
        }
    }
}

@available(*, unavailable)
extension PostgresNotificationSequence.AsyncIterator: Sendable {}
