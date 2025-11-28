/// Handle to send data for a `COPY ... FROM STDIN` query to the backend.
public struct PostgresCopyFromWriter: Sendable {
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
        promise.futureResult.flatMap {
            if self.eventLoop.inEventLoop {
                return self.eventLoop.makeCompletedFuture(withResultOf: {
                    try self.channelHandler.value.sendCopyData(byteBuffer)
                })
            } else {
                let promise = self.eventLoop.makePromise(of: Void.self)
                self.eventLoop.execute {
                    promise.completeWith(Result(catching: { try self.channelHandler.value.sendCopyData(byteBuffer) }))
                }
                return promise.futureResult
            }
        }.whenComplete { result in
            continuation.resume(with: result)
        }
    }

    /// Send data for a `COPY ... FROM STDIN` operation to the backend.
    ///
    /// - Throws: If an error occurs during the write of if the backend sent an `ErrorResponse` during the copy
    ///   operation, eg. to indicate that a **previous** `write` call had an invalid format.
    public func write(_ byteBuffer: ByteBuffer, isolation: isolated (any Actor)? = #isolation) async throws {
        // Check for cancellation. This is cheap and makes sure that we regularly check for cancellation in the
        // `writeData` closure. It is likely that the user would forget to do so.
        try Task.checkCancellation()

        try await withTaskCancellationHandler {
            do {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                    if self.eventLoop.inEventLoop {
                        writeAssumingInEventLoop(byteBuffer, continuation)
                    } else {
                        self.eventLoop.execute {
                            writeAssumingInEventLoop(byteBuffer, continuation)
                        }
                    }
                }
            } catch {
                if Task.isCancelled {
                    // If the task was cancelled, we might receive a postgres error which is an artifact about how we
                    // communicate the cancellation to the state machine. Throw a `CancellationError` to the user
                    // instead, which looks more like native Swift Concurrency code.
                    throw CancellationError()
                }
                throw error
            }
        } onCancel: {
            if self.eventLoop.inEventLoop {
                self.channelHandler.value.cancel()
            } else {
                self.eventLoop.execute {
                    self.channelHandler.value.cancel()
                }
            }
        }
    }

    /// Finalize the data transfer, putting the state machine out of the copy mode and sending a `CopyDone` message to
    /// the backend.
    func done(isolation: isolated (any Actor)? = #isolation) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            if self.eventLoop.inEventLoop {
                self.channelHandler.value.sendCopyDone(continuation: continuation)
            } else {
                self.eventLoop.execute {
                    self.channelHandler.value.sendCopyDone(continuation: continuation)
                }
            }
        }
    }

    /// Finalize the data transfer, putting the state machine out of the copy mode and sending a `CopyFail` message to
    /// the backend.
    func failed(error: any Error, isolation: isolated (any Actor)? = #isolation) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
            if self.eventLoop.inEventLoop {
                self.channelHandler.value.sendCopyFail(message: "Client failed copy", continuation: continuation)
            } else {
                self.eventLoop.execute {
                    self.channelHandler.value.sendCopyFail(message: "Client failed copy", continuation: continuation)
                }
            }
        }
    }
}

/// Specifies the format in which data is transferred to the backend in a COPY operation.
///
/// See the Postgres documentation at https://www.postgresql.org/docs/current/sql-copy.html for the option's meanings
/// and their default values.
public struct PostgresCopyFromFormat: Sendable {
    /// Options that can be used to modify the `text` format of a COPY operation.
    public struct TextOptions: Sendable {
        /// The delimiter that separates columns in the data.
        ///
        /// See the `DELIMITER` option in Postgres's `COPY` command.
        public var delimiter: UnicodeScalar? = nil

        public init() {}
    }

    enum Format {
        case text(TextOptions)
    }

    var format: Format

    public static func text(_ options: TextOptions) -> PostgresCopyFromFormat {
        return PostgresCopyFromFormat(format: .text(options))
    }
}

/// Create a `COPY ... FROM STDIN` query based on the given parameters.
///
/// An empty `columns` array signifies that no columns should be specified in the query and that all columns will be
/// copied by the caller.
///
/// - Warning: The table and column names are inserted into the `COPY FROM` query as passed and might thus be
///   susceptible to SQL injection. Ensure no untrusted data is contained in these strings.
private func buildCopyFromQuery(
    table: String,
    columns: [String] = [],
    format: PostgresCopyFromFormat
) -> PostgresQuery {
    var query = """
        COPY "\(table)"
        """
    if !columns.isEmpty {
        query += "("
        query += columns.map { #""\#($0)""# }.joined(separator: ",")
        query += ")"
    }
    query += " FROM STDIN"
    var queryOptions: [String] = []
    switch format.format {
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
    /// - Important: The table and column names are inserted into the `COPY FROM` query as passed and might thus be
    ///   susceptible to SQL injection. Ensure no untrusted data is contained in these strings.
    public func copyFrom(
        table: String,
        columns: [String] = [],
        format: PostgresCopyFromFormat = .text(.init()),
        logger: Logger,
        isolation: isolated (any Actor)? = #isolation,
        file: String = #fileID,
        line: Int = #line,
        writeData: (PostgresCopyFromWriter) async throws -> Void
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
            //    a `Sync` that takes the backend out of copy mode. If `writeData` threw the error from from the
            //    `PostgresCopyFromWriter.write` call, `writer.failed` will throw with the same error, so it doesn't
            //    matter that we ignore the error here. If the user threw some other error, it's better to honor the
            //    user's error.
            try? await writer.failed(error: error)

            throw error
        }

        // `writer.done` may fail, eg. because the backend sends an error response after receiving `CopyDone` or during
        // the transfer of the last bit of data so that the user didn't call `PostgresCopyFromWriter.write` again, which
        // would have checked the error state. In either of these cases, calling `writer.done` puts the backend out of
        // copy mode, so we don't need to send another `CopyFail`. Thus, this must not be handled in the `do` block
        // above.
        try await writer.done()
    }
}
