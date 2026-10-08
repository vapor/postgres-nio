import Atomics
import NIOCore
import NIOConcurrencyHelpers

/// An async sequence of ``PostgresRow``s.
///
/// - Note: This is a struct to allow us to move to a move-only type easily once they become available.
public struct PostgresRowSequence: AsyncSequence, Sendable {
    public typealias Element = PostgresRow

    typealias BackingSequence = NIOThrowingAsyncSequenceProducer<DataRow, any Error, AdaptiveRowBuffer, PSQLRowStream>

    let backing: BackingSequence
    let lookupTable: [String: Int]
    let _columns: [RowDescription.Column]
    var scope: Scope?

    init(_ backing: BackingSequence, lookupTable: [String: Int], columns: [RowDescription.Column]) {
        self.backing = backing
        self.lookupTable = lookupTable
        self._columns = columns
        self.scope = nil
    }

    func scoped(to scope: Scope) -> PostgresRowSequence {
        var sequence = self
        sequence.scope = scope
        return sequence
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            backing: self.backing.makeAsyncIterator(),
            lookupTable: self.lookupTable,
            columns: self._columns,
            scope: self.scope
        )
    }
}

extension PostgresRowSequence {
    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = PostgresRow
        public typealias Failure = any Error

        let backing: BackingSequence.AsyncIterator

        let lookupTable: [String: Int]
        let columns: [RowDescription.Column]
        let scope: Scope?

        init(backing: BackingSequence.AsyncIterator, lookupTable: [String: Int], columns: [RowDescription.Column], scope: Scope?) {
            self.backing = backing
            self.lookupTable = lookupTable
            self.columns = columns
            self.scope = scope
        }

        @concurrent
        public mutating func next() async throws -> Element? {
            self.scope?.preconditionOpen()
            defer {
                // re-check: the scope may have changed while we were suspended
                self.scope?.preconditionOpen()
            }
            if let dataRow = try await self.backing.next() {
                return PostgresRow(
                    data: dataRow,
                    lookupTable: self.lookupTable,
                    columns: self.columns
                )
            }
            return nil
        }

        @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
        public mutating func next(isolation actor: isolated (any Actor)?) async throws(Self.Failure) -> PostgresRow? {
            // Since the underlying NIOThrowingAsyncSequenceProducer<DataRow, Error, AdaptiveRowBuffer, PSQLRowStream>.AsyncIterator
            // does not supported the next(isolation:) call yet, we will hop here back and forth.
            struct UnsafeTransfer: @unchecked Sendable {
                var backing: BackingSequence.AsyncIterator
            }
            self.scope?.preconditionOpen()
            defer {
                // re-check: the scope may have changed while we were suspended
                self.scope?.preconditionOpen()
            }
            let unsafeTransfer = UnsafeTransfer(backing: self.backing)
            if let dataRow = try await unsafeTransfer.backing.next() {
                return PostgresRow(
                    data: dataRow,
                    lookupTable: self.lookupTable,
                    columns: self.columns
                )
            }
            return nil
        }
    }
}

extension PostgresRowSequence {
    /// Tracks whether the `query` closure a ``PostgresRowSequence`` was passed to has returned.
    /// Consuming the sequence after the scope is closed is a programmer error and triggers a precondition failure.
    final class Scope: Sendable {
        private let isClosed = ManagedAtomic(false)
        let file: String
        let line: Int

        init(file: String, line: Int) {
            self.file = file
            self.line = line
        }

        func close() {
            self.isClosed.store(true, ordering: .releasing)
        }

        func preconditionOpen() {
            if self.isClosed.load(ordering: .acquiring) {
                preconditionFailure("A PostgresRowSequence was consumed after the closure of the query started at \(self.file):\(self.line) returned. The sequence must not escape the closure it was passed to.")
            }
        }

        struct ClosedError: Error {}
    }
}

@available(*, unavailable)
extension PostgresRowSequence.AsyncIterator: Sendable {}

extension PostgresRowSequence {
    public func collect() async throws -> [PostgresRow] {
        var result = [PostgresRow]()
        for try await row in self {
            result.append(row)
        }
        return result
    }
}

struct AdaptiveRowBuffer: NIOAsyncSequenceProducerBackPressureStrategy {
    static let defaultBufferTarget = 256
    static let defaultBufferMinimum = 1
    static let defaultBufferMaximum = 16384

    let minimum: Int
    let maximum: Int

    private var target: Int
    private var canShrink: Bool = false

    init(minimum: Int, maximum: Int, target: Int) {
        precondition(minimum <= target && target <= maximum)
        self.minimum = minimum
        self.maximum = maximum
        self.target = target
    }

    init() {
        self.init(
            minimum: Self.defaultBufferMinimum,
            maximum: Self.defaultBufferMaximum,
            target: Self.defaultBufferTarget
        )
    }

    mutating func didYield(bufferDepth: Int) -> Bool {
        if bufferDepth > self.target, self.canShrink, self.target > self.minimum {
            self.target &>>= 1
        }
        self.canShrink = true

        return false // bufferDepth < self.target
    }

    mutating func didConsume(bufferDepth: Int) -> Bool {
        // If the buffer is drained now, we should double our target size.
        if bufferDepth == 0, self.target < self.maximum {
            self.target = self.target * 2
            self.canShrink = false
        }

        return bufferDepth < self.target
    }
}
