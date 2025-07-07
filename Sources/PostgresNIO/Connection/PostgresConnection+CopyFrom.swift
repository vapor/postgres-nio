/// Handle to send data for a `COPY ... FROM STDIN` query to the backend.
public struct PostgresCopyFromWriter: Sendable {
    /// The backend failed the copy data transfer, which means that no more data sent by the frontend would be processed.
    ///
    /// The `PostgresCopyFromWriter` should cancel the data transfer.
    public struct CopyCancellationError: Error {
        /// The error that the backend sent us which cancelled the data transfer.
        ///
        /// Note that this error is related to previous `write` calls since a `CopyCancellationError` is thrown before
        /// new data is written by `write`.
        public let underlyingError: PSQLError
    }

    private let channelHandler: NIOLoopBound<PostgresChannelHandler>
    private let eventLoop: any EventLoop

    init(handler: PostgresChannelHandler, eventLoop: any EventLoop) {
        self.channelHandler = NIOLoopBound(handler, eventLoop: eventLoop)
        self.eventLoop = eventLoop
    }

    private func writeAssumingInEventLoop(_ byteBuffer: ByteBuffer, _ continuation: CheckedContinuation<Void, any Error>) {
        precondition(eventLoop.inEventLoop)
        let promise = eventLoop.makePromise(of: Void.self)
        self.channelHandler.value.checkBackendCanReceiveCopyData(promise: promise)
        promise.futureResult.map {
            if eventLoop.inEventLoop {
                self.channelHandler.value.sendCopyData(byteBuffer)
            } else {
                eventLoop.execute {
                    self.channelHandler.value.sendCopyData(byteBuffer)
                }
            }
        }.whenComplete { result in
            continuation.resume(with: result)
        }
    }

    /// Send data for a `COPY ... FROM STDIN` operation to the backend.
    ///
    /// If the backend encountered an error during the data transfer and thus cannot process any more data, this throws
    /// a `CopyCancellationError`.
    public func write(_ byteBuffer: ByteBuffer) async throws {
        // Check for cancellation. This is cheap and makes sure that we regularly check for cancellation in the
        // `writeData` closure. It is likely that the user would forget to do so.
        try Task.checkCancellation()

        // TODO: Listen for task cancellation while we are waiting for backpressure to clear.
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            if eventLoop.inEventLoop {
                writeAssumingInEventLoop(byteBuffer, continuation)
            } else {
                eventLoop.execute {
                    writeAssumingInEventLoop(byteBuffer, continuation)
                }
            }
        }
    }

    /// Finalize the data transfer, putting the state machine out of the copy mode and sending a `CopyDone` message to
    /// the backend.
    func done() async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            if eventLoop.inEventLoop {
                self.channelHandler.value.sendCopyDone(continuation: continuation)
            } else {
                eventLoop.execute {
                    self.channelHandler.value.sendCopyDone(continuation: continuation)
                }
            }
        }
    }

    /// Finalize the data transfer, putting the state machine out of the copy mode and sending a `CopyFail` message to
    /// the backend.
    func failed(error: any Error) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            // TODO: Is it OK to use string interpolation to construct an error description to be sent to the backend
            // here? We could also use a generic description, it doesn't really matter since we throw the user's error
            // in `copyFrom`.
            if eventLoop.inEventLoop {
                self.channelHandler.value.sendCopyFail(message: "\(error)", continuation: continuation)
            } else {
                eventLoop.execute {
                    self.channelHandler.value.sendCopyFail(message: "\(error)", continuation: continuation)
                }
            }
        }
    }
}

/// Specifies the format in which data is transferred to the backend in a COPY operation.
public enum PostgresCopyFromFormat: Sendable {
    /// Options that can be used to modify the `text` format of a COPY operation.
    public struct TextOptions: Sendable {
        /// The delimiter that separates columns in the data.
        ///
        /// See the `DELIMITER` option in Postgres's `COPY` command.
        ///
        /// Uses the default delimiter of the format
        public var delimiter: UnicodeScalar? = nil

        public init() {}
    }

    case text(TextOptions)
}

/// Create a `COPY ... FROM STDIN` query based on the given parameters.
///
/// An empty `columns` array signifies that no columns should be specified in the query and that all columns will be
/// copied by the caller.
private func buildCopyFromQuery(
    table: StaticString,
    columns: [StaticString] = [],
    format: PostgresCopyFromFormat
) -> PostgresQuery {
    // TODO: Should we put the table and column names in quotes to make them case-sensitive?
    var query = "COPY \(table)"
    if !columns.isEmpty {
        query += "(" + columns.map(\.description).joined(separator: ",") + ")"
    }
    query += " FROM STDIN"
    var queryOptions: [String] = []
    switch format {
    case .text(let options):
        queryOptions.append("FORMAT text")
        if let delimiter = options.delimiter {
            // Set the delimiter as a Unicode code point. This avoids the possibility of SQL injection.
            queryOptions.append("DELIMITER U&'\\\(String(format: "%04x", delimiter.value))'")
        }
    }
    precondition(!queryOptions.isEmpty)
    query += " WITH ("
    query += queryOptions.map { "\($0)" }.joined(separator: ",")
    query += ")"
    return "\(unescaped: query)"
}

extension PostgresConnection {
    /// Copy data into a table using a `COPY <table name> FROM STDIN` query.
    ///
    /// - Parameters:
    ///   - table: The name of the table into which to copy the data.
    ///   - columns: The name of the columns to copy. If an empty array is passed, all columns are assumed to be copied.
    ///   - format: Options that specify the format of the data that is produced by `writeData`.
    ///   - writeData: Closure that produces the data for the table, to be streamed to the backend. Call `write` on the
    ///     writer provided by the closure to send data to the backend and return from the closure once all data is sent.
    ///     Throw an error from the closure to fail the data transfer. The error thrown by the closure will be rethrown
    ///     by the `copyFrom` function.
    ///
    /// - Note: The table and column names are inserted into the SQL query verbatim. They are forced to be compile-time
    ///   specified to avoid runtime SQL injection attacks.
    public func copyFrom(
        table: StaticString,
        columns: [StaticString] = [],
        format: PostgresCopyFromFormat = .text(.init()),
        logger: Logger,
        file: String = #fileID,
        line: Int = #line,
        writeData: @escaping @Sendable (PostgresCopyFromWriter) async throws -> Void
    )  async throws {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.id)"
        let writer: PostgresCopyFromWriter = try await withCheckedThrowingContinuation { continuation in
            let context = ExtendedQueryContext(
                copyFromQuery: buildCopyFromQuery(table: table, columns: columns, format: format),
                triggerCopy: continuation,
                logger: logger
            )
            self.channel.write(HandlerTask.extendedQuery(context), promise: nil)
        }

        do {
            try await writeData(writer)
        } catch {
            // We need to send a `CopyFail` to the backend to put it out of copy mode. This will most likely throw, most
            // notably for the following two reasons. In both of them, it's better to ignore the error thrown by
            // `writer.failed` and instead throw the error from `writeData`:
            //  - We send `CopyFail` and the backend replies with an `ErrorResponse` that relays the `CopyFail` message.
            //    This took the backend out of copy mode but it's more informative to the user to see the error they
            //    threw instead of the one that got relayed back, so it's better to ignore the error here.
            //  - The backend sent us an `ErrorResponse` during the copy, eg. because of an invalid format. This puts
            //    the `ExtendedQueryStateMachine` in the error state. Trying to send a `CopyFail` will throw but trigger
            //    a `Sync` that takes the backend out of copy mode. If `writeData` threw the `CopyCancellationError`
            //    from the `PostgresCopyFromWriter.write` call, `writer.failed` will throw with the same error, so it
            //    doesn't matter that we ignore the error here. If the user threw some other error, it's better to honor
            //    the user's error.
            try? await writer.failed(error: error)

            if let error = error as? PostgresCopyFromWriter.CopyCancellationError {
                // If we receive a `CopyCancellationError` that is with almost certain likelihood because
                // `PostgresCopyFromWriter.write` threw it - otherwise the user must have saved a previous
                // `PostgresCopyFromWriter` error, which is very unlikely.
                // Throw the underlying error because that contains the error message that was sent by the backend and
                // is most actionable by the user.
                throw error.underlyingError
            } else {
                throw error
            }
        }

        // `writer.done` may fail, eg. because the backend sends an error response after receiving `CopyDone` or during
        // the transfer of the last bit of data so that the user didn't call `PostgresCopyFromWriter.write` again, which
        // would have checked the error state. In either of these cases, calling `writer.done` puts the backend out of
        // copy mode, so we don't need to send another `CopyFail`. Thus, this must not be handled in the `do` block
        // above.
        try await writer.done()
    }

}
