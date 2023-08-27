import NIOCore

struct PostgresFrontendMessageEncoder {
    private enum State {
        case flushed
        case writable
    }
    
    private var buffer: ByteBuffer
    private var state: State = .writable
    
    init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    mutating func startup(_ parameters: PostgresFrontendMessage.Startup.Parameters) {
        self.clearIfNeeded()
        self.encodeLengthPrefixed { buffer in
            buffer.writeInteger(PostgresFrontendMessage.Startup.versionThree)
            buffer.writeNullTerminatedString("user")
            buffer.writeNullTerminatedString(parameters.user)

            if let database = parameters.database {
                buffer.writeNullTerminatedString("database")
                buffer.writeNullTerminatedString(database)
            }

            if let options = parameters.options {
                buffer.writeNullTerminatedString("options")
                buffer.writeNullTerminatedString(options)
            }

            switch parameters.replication {
            case .database:
                buffer.writeNullTerminatedString("replication")
                buffer.writeNullTerminatedString("replication")
            case .true:
                buffer.writeNullTerminatedString("replication")
                buffer.writeNullTerminatedString("true")
            case .false:
                break
            }

            buffer.writeInteger(UInt8(0))
        }
    }

    mutating func bind(portalName: String, preparedStatementName: String, bind: PostgresBindings) {
        self.clearIfNeeded()
        self.buffer.psqlWriteFrontendMessageID(.bind)
        self.encodeLengthPrefixed { buffer in
            buffer.writeNullTerminatedString(portalName)
            buffer.writeNullTerminatedString(preparedStatementName)

            // The number of parameter format codes that follow (denoted C below). This can be
            // zero to indicate that there are no parameters or that the parameters all use the
            // default format (text); or one, in which case the specified format code is applied
            // to all parameters; or it can equal the actual number of parameters.
            buffer.writeInteger(UInt16(bind.count))

            // The parameter format codes. Each must presently be zero (text) or one (binary).
            bind.metadata.forEach {
                buffer.writeInteger($0.format.rawValue)
            }

            buffer.writeInteger(UInt16(bind.count))

            var parametersCopy = bind.bytes
            buffer.writeBuffer(&parametersCopy)

            // The number of result-column format codes that follow (denoted R below). This can be
            // zero to indicate that there are no result columns or that the result columns should
            // all use the default format (text); or one, in which case the specified format code
            // is applied to all result columns (if any); or it can equal the actual number of
            // result columns of the query.
            buffer.writeInteger(1, as: Int16.self)
            // The result-column format codes. Each must presently be zero (text) or one (binary).
            buffer.writeInteger(PostgresFormat.binary.rawValue, as: Int16.self)
        }
    }

    mutating func cancel(processID: Int32, secretKey: Int32) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(UInt32(16), PostgresFrontendMessage.Cancel.requestCode, processID, secretKey)
    }

    mutating func closePreparedStatement(_ preparedStatement: String) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.close.rawValue, UInt32(6 + preparedStatement.utf8.count), UInt8(ascii: "S"))
        self.buffer.writeNullTerminatedString(preparedStatement)
    }

    mutating func closePortal(_ portal: String) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.close.rawValue, UInt32(6 + portal.utf8.count), UInt8(ascii: "P"))
        self.buffer.writeNullTerminatedString(portal)
    }

    mutating func describePreparedStatement(_ preparedStatement: String) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.describe.rawValue, UInt32(6 + preparedStatement.utf8.count), UInt8(ascii: "S"))
        self.buffer.writeNullTerminatedString(preparedStatement)
    }

    mutating func describePortal(_ portal: String) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.describe.rawValue, UInt32(6 + portal.utf8.count), UInt8(ascii: "P"))
        self.buffer.writeNullTerminatedString(portal)
    }

    mutating func execute(portalName: String, maxNumberOfRows: Int32 = 0) {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.execute.rawValue, UInt32(9 + portalName.utf8.count))
        self.buffer.writeNullTerminatedString(portalName)
        self.buffer.writeInteger(maxNumberOfRows)
    }

    mutating func parse<Parameters: Collection>(preparedStatementName: String, query: String, parameters: Parameters) where Parameters.Element == PostgresDataType {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(
            PostgresFrontendMessage.ID.parse.rawValue,
            UInt32(4 + preparedStatementName.utf8.count + 1 + query.utf8.count + 1 + 2 + MemoryLayout<PostgresDataType>.size * parameters.count)
        )
        self.buffer.writeNullTerminatedString(preparedStatementName)
        self.buffer.writeNullTerminatedString(query)
        self.buffer.writeInteger(UInt16(parameters.count))

        for dataType in parameters {
            self.buffer.writeInteger(dataType.rawValue)
        }
    }

    mutating func password<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.password.rawValue, UInt32(5 + bytes.count))
        self.buffer.writeBytes(bytes)
        self.buffer.writeInteger(UInt8(0))
    }

    mutating func flush() {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.flush.rawValue, UInt32(4))
    }

    mutating func saslResponse<Bytes: Collection>(_ bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.saslResponse.rawValue, UInt32(4 + bytes.count))
        self.buffer.writeBytes(bytes)
    }

    mutating func saslInitialResponse<Bytes: Collection>(mechanism: String, bytes: Bytes) where Bytes.Element == UInt8 {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(
            PostgresFrontendMessage.ID.saslInitialResponse.rawValue,
            UInt32(4 + mechanism.utf8.count + 1 + 4 + bytes.count)
        )
        self.buffer.writeNullTerminatedString(mechanism)
        if bytes.count > 0 {
            self.buffer.writeInteger(Int32(bytes.count))
            self.buffer.writeBytes(bytes)
        } else {
            self.buffer.writeInteger(Int32(-1))
        }
    }

    mutating func ssl() {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(UInt32(8), PostgresFrontendMessage.SSLRequest.requestCode)
    }

    mutating func sync() {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.sync.rawValue, UInt32(4))
    }

    mutating func terminate() {
        self.clearIfNeeded()
        self.buffer.writeMultipleIntegers(PostgresFrontendMessage.ID.terminate.rawValue, UInt32(4))
    }

    mutating func flushBuffer() -> ByteBuffer {
        self.state = .flushed
        return self.buffer
    }

    private mutating func clearIfNeeded() {
        switch self.state {
        case .flushed:
            self.state = .writable
            self.buffer.clear()

        case .writable:
            break
        }
    }

    private mutating func encodeLengthPrefixed(_ encode: (inout ByteBuffer) -> ()) {
        let startIndex = self.buffer.writerIndex
        self.buffer.writeInteger(UInt32(0)) // placeholder for length
        encode(&self.buffer)
        let length = UInt32(self.buffer.writerIndex - startIndex)
        self.buffer.setInteger(length, at: startIndex)
    }

}
