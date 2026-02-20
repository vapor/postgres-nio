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
    public func write(_ byteBuffer: ByteBuffer) async throws {
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
    func done() async throws {
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
    func failed(error: any Error) async throws {
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

// PostgresBinaryCopyFromWriter relies on non-Escapable types, which were only introduced in Swift 6.2
#if compiler(>=6.2)
/// Handle to send binary data for a `COPY ... FROM STDIN` query to the backend.
///
/// It takes care of serializing `PostgresEncodable` column types into the binary format that Postgres expects.
public struct PostgresBinaryCopyFromWriter: ~Copyable {
    /// Handle to serialize columns into a row that is being written by `PostgresBinaryCopyFromWriter`.
    public struct ColumnWriter: ~Escapable, ~Copyable {
        /// Pointer to the `PostgresBinaryCopyFromWriter` that is gathering the serialized data.
        @usableFromInline
        let underlying: UnsafeMutablePointer<PostgresBinaryCopyFromWriter>

        /// The number of columns that have been written by this `ColumnWriter`.
        @usableFromInline
        var columns: UInt16 = 0

        /// - Warning: Do not call directly, call `withColumnWriter` instead
        @usableFromInline
        init(_underlying: UnsafeMutablePointer<PostgresBinaryCopyFromWriter>) {
            self.underlying = _underlying
        }

        @usableFromInline
        static func withColumnWriter<T, E: Error>(
            writingTo underlying: inout PostgresBinaryCopyFromWriter,
            body: (inout ColumnWriter) throws(E) -> T
        ) throws(E) -> T {
            return try withUnsafeMutablePointer(to: &underlying) { pointerToUnderlying throws(E) in
                // We can guarantee that `ColumWriter` never outlives `underlying` because `ColumnWriter` is
                // `~Escapable` and thus cannot escape the context of the closure to `withUnsafeMutablePointer`.
                // To model this without resorting to unsafe pointers, we would need to be able to declare an `inout`
                // reference to `PostgresBinaryCopyFromWriter` as a member of `ColumnWriter`, which isn't possible at
                // the moment (https://github.com/swiftlang/swift/issues/85832).
                var columnWriter = ColumnWriter(_underlying: pointerToUnderlying)
                return try body(&columnWriter)
            }
        }

        /// Serialize a single column to a row.
        ///
        /// - Important: It is critical that that data type encoded here exactly matches the data type in the
        ///   database. For example, if the database stores an a 4-bit integer the corresponding `writeColumn` must
        ///   be called with an `Int32`. Serializing an integer of a different width will cause a deserialization
        ///   failure in the backend.
        @inlinable
        #if compiler(<6.3)
        @_lifetime(&self)
        #endif
        public mutating func writeColumn(_ column: (some PostgresEncodable)?) throws {
            columns += 1
            try invokeWriteColumn(on: underlying, column)
        }

        // Needed to work around https://github.com/swiftlang/swift/issues/83309, copying the implementation into
        // `writeColumn` causes an assertion failure when thread sanitizer is enabled.
        @inlinable
        func invokeWriteColumn(
            on writer: UnsafeMutablePointer<PostgresBinaryCopyFromWriter>,
            _ column: (some PostgresEncodable)?
        ) throws {
            try writer.pointee.writeColumn(column)
        }
    }

    /// The underlying `PostgresCopyFromWriter` that sends the serialized data to the backend.
    @usableFromInline let underlying: PostgresCopyFromWriter

    /// The buffer in which we accumulate binary data. Once this buffer exceeds `bufferSize`, we flush it to
    /// the backend.
    @usableFromInline var buffer = ByteBuffer()

    /// Once `buffer` exceeds this size, it gets flushed to the backend.
    @usableFromInline let bufferSize: Int

    init(underlying: PostgresCopyFromWriter, bufferSize: Int) {
        self.underlying = underlying
        // Allocate 10% more than the buffer size because we only flush the buffer once it has exceeded `bufferSize`
        buffer.reserveCapacity(bufferSize + bufferSize / 10)
        self.bufferSize = bufferSize
    }

    /// Serialize a single row to the backend. Call `writeColumn` on `columnWriter` for every column that should be
    /// included in the row.
    @inlinable
    public mutating func writeRow<Result>(_ body: (_ columnWriter: inout ColumnWriter) throws -> Result) async throws -> Result {
        // Write a placeholder for the number of columns
        let columnIndex = buffer.writerIndex
        buffer.writeInteger(UInt16(0))

        let (columns, bodyResult) = try ColumnWriter.withColumnWriter(writingTo: &self) { columnWriter in
            let bodyResult = try body(&columnWriter)
            return (columnWriter.columns, bodyResult)
        }

        // Fill in the number of columns
        buffer.setInteger(columns, at: columnIndex)

        if buffer.readableBytes > bufferSize {
            try await flush()
        }
        return bodyResult
    }

    /// Serialize a single column to the buffer. Should only be called by `ColumnWriter`.
    @inlinable
    mutating func writeColumn(_ column: (some PostgresEncodable)?) throws {
        guard let column else {
            buffer.writeInteger(Int32(-1))
            return
        }
        try buffer.writeLengthPrefixed(as: Int32.self) { buffer in
            let startIndex = buffer.writerIndex
            try column.encode(into: &buffer, context: .default)
            return buffer.writerIndex - startIndex
        }
    }

    /// Flush any pending data in the buffer to the backend.
    @usableFromInline
    mutating func flush() async throws {
        try await underlying.write(buffer)
        buffer.clear()
    }
}
#endif

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

    /// Options that can be used to modify the `binary` format of a COPY operation.
    public struct BinaryOptions: Sendable {
        public init() {}
    }

    enum Format {
        case text(TextOptions)
        case binary(BinaryOptions)
    }

    var format: Format

    /// Copy data to Postgres in text format, eg. separated by comma.
    public static func text(_ options: TextOptions) -> PostgresCopyFromFormat {
        return PostgresCopyFromFormat(format: .text(options))
    }

    /// Copy data to Postgres in binary format.
    public static func binary(_ options: BinaryOptions) -> PostgresCopyFromFormat {
        return PostgresCopyFromFormat(format: .binary(options))
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
    case .binary:
        queryOptions.append("FORMAT binary")
    }
    precondition(!queryOptions.isEmpty)
    query += " WITH ("
    query += queryOptions.map { "\($0)" }.joined(separator: ",")
    query += ")"
    return "\(unescaped: query)"
}

extension PostgresConnection {
    #if compiler(>=6.2)
    /// Copy data into a table using a `COPY <table name> FROM STDIN` query, transferring data in a binary format.
    ///
    /// - Parameters:
    ///   - table: The name of the table into which to copy the data.
    ///   - columns: The name of the columns to copy. If an empty array is passed, all columns are assumed to be copied.
    ///   - bufferSize: How many bytes to accumulate a local buffer before flushing it to the database. Can affect
    ///     performance characteristics of the copy operation.
    ///   - writeData: Closure that produces the data for the table, to be streamed to the backend. Call `write` on the
    ///     writer provided by the closure to send data to the backend and return from the closure once all data is sent.
    ///     Throw an error from the closure to fail the data transfer. The error thrown by the closure will be rethrown
    ///     by the `copyFromBinary` function.
    ///
    /// - Important: The table and column names are inserted into the `COPY FROM` query as passed and might thus be
    ///   susceptible to SQL injection. Ensure no untrusted data is contained in these strings.
    public func copyFromBinary(
        table: String,
        columns: [String] = [],
        options: PostgresCopyFromFormat.BinaryOptions = .init(),
        bufferSize: Int = 100_000,
        logger: Logger,
        file: String = #fileID,
        line: Int = #line,
        writeData: (inout PostgresBinaryCopyFromWriter) async throws -> Void
    )  async throws {
        try await copyFrom(table: table, columns: columns, format: .binary(PostgresCopyFromFormat.BinaryOptions()), logger: logger) { writer in
            var header = ByteBuffer()
            header.reserveCapacity(19)
            header.writeString("PGCOPY\n")
            header.writeInteger(UInt8(0xff))
            header.writeString("\r\n\0")

            // Flag fields
            header.writeInteger(UInt32(0))

            // Header extension area length
            header.writeInteger(UInt32(0))
            try await writer.write(header)

            var binaryWriter = PostgresBinaryCopyFromWriter(underlying: writer, bufferSize: bufferSize)
            try await writeData(&binaryWriter)
            try await binaryWriter.flush()
        }
    }
    #endif

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
