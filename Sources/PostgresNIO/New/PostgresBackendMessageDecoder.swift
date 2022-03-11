struct PostgresBackendMessageDecoder: NIOSingleStepByteToMessageDecoder {
    typealias InboundOut = PostgresBackendMessage
    
    private(set) var hasAlreadyReceivedBytes: Bool
    
    init(hasAlreadyReceivedBytes: Bool = false) {
        self.hasAlreadyReceivedBytes = hasAlreadyReceivedBytes
    }
    
    mutating func decode(buffer: inout ByteBuffer) throws -> PostgresBackendMessage? {
        
        if !self.hasAlreadyReceivedBytes {
            // We have not received any bytes yet! Let's peek at the first message id. If it
            // is a "S" or "N" we assume that it is connected to an SSL upgrade request. All
            // other messages that we expect now, don't start with either "S" or "N"
            
            let startReaderIndex = buffer.readerIndex
            guard let firstByte = buffer.readInteger(as: UInt8.self) else {
                return nil
            }
            
            switch firstByte {
            case UInt8(ascii: "S"):
                self.hasAlreadyReceivedBytes = true
                return .sslSupported
                
            case UInt8(ascii: "N"):
                self.hasAlreadyReceivedBytes = true
                return .sslUnsupported
                
            default:
                // move reader index back
                buffer.moveReaderIndex(to: startReaderIndex)
                self.hasAlreadyReceivedBytes = true
            }
        }
        
        // all other packages start with a MessageID (UInt8) and their message length (UInt32).
        // do we have enough bytes for that?
        let startReaderIndex = buffer.readerIndex
        guard let (idByte, length) = buffer.readMultipleIntegers(endianness: .big, as: (UInt8, UInt32).self) else {
            // if this fails, the readerIndex wasn't changed
            return nil
        }
        
        // 1. try to read the message
        guard var message = buffer.readSlice(length: Int(length) - 4) else {
            // we need to move the reader index back to its start point
            buffer.moveReaderIndex(to: startReaderIndex)
            return nil
        }
        
        // 2. make sure we have a known message identifier
        guard let messageID = PostgresBackendMessage.ID(rawValue: idByte) else {
            buffer.moveReaderIndex(to: startReaderIndex)
            let completeMessage = buffer.readSlice(length: Int(length) + 1)!
            throw PSQLDecodingError.unknownMessageIDReceived(messageID: idByte, messageBytes: completeMessage)
        }
        
        // 3. decode the message
        do {
            let result = try PostgresBackendMessage.decode(from: &message, for: messageID)
            if message.readableBytes > 0 {
                throw PSQLPartialDecodingError.expectedExactlyNRemainingBytes(0, actual: message.readableBytes)
            }
            return result
        } catch let error as PSQLPartialDecodingError {
            buffer.moveReaderIndex(to: startReaderIndex)
            let completeMessage = buffer.readSlice(length: Int(length) + 1)!
            throw PSQLDecodingError.withPartialError(error, messageID: messageID.rawValue, messageBytes: completeMessage)
        } catch {
            preconditionFailure("Expected to only see `PartialDecodingError`s here.")
        }
    }
    
    mutating func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> PostgresBackendMessage? {
        try self.decode(buffer: &buffer)
    }
}


    
/// An error representing a failure to decode [a Postgres wire message](https://www.postgresql.org/docs/13/protocol-message-formats.html)
/// to the Swift structure `PSQLBackendMessage`.
///
/// If you encounter a `DecodingError` when using a trusted Postgres server please make to file an issue at:
/// [https://github.com/vapor/postgres-nio/issues](https://github.com/vapor/postgres-nio/issues)
struct PSQLDecodingError: Error {
    
    /// The backend message ID bytes
    let messageID: UInt8
    
    /// The backend message's payload encoded in base64
    let payload: String
    
    /// A textual description of the error
    let description: String
    
    /// The file this error was thrown in
    let file: String
    
    /// The line in `file` this error was thrown
    let line: Int
    
    static func withPartialError(
        _ partialError: PSQLPartialDecodingError,
        messageID: UInt8,
        messageBytes: ByteBuffer) -> Self
    {
        var byteBuffer = messageBytes
        let data = byteBuffer.readData(length: byteBuffer.readableBytes)!
        
        return PSQLDecodingError(
            messageID: messageID,
            payload: data.base64EncodedString(),
            description: partialError.description,
            file: partialError.file,
            line: partialError.line)
    }
    
    static func unknownMessageIDReceived(
        messageID: UInt8,
        messageBytes: ByteBuffer,
        file: String = #file,
        line: Int = #line) -> Self
    {
        var byteBuffer = messageBytes
        let data = byteBuffer.readData(length: byteBuffer.readableBytes)!
        
        return PSQLDecodingError(
            messageID: messageID,
            payload: data.base64EncodedString(),
            description: "Received a message with messageID '\(Character(UnicodeScalar(messageID)))'. There is no message type associated with this message identifier.",
            file: file,
            line: line)
    }
    
}

struct PSQLPartialDecodingError: Error {
    /// A textual description of the error
    let description: String
    
    /// The file this error was thrown in
    let file: String
    
    /// The line in `file` this error was thrown
    let line: Int
    
    static func valueNotRawRepresentable<Target: RawRepresentable>(
        value: Target.RawValue,
        asType: Target.Type,
        file: String = #file,
        line: Int = #line) -> Self
    {
        return PSQLPartialDecodingError(
            description: "Can not represent '\(value)' with type '\(asType)'.",
            file: file, line: line)
    }
    
    static func unexpectedValue(value: Any, file: String = #file, line: Int = #line) -> Self {
        return PSQLPartialDecodingError(
            description: "Value '\(value)' is not expected.",
            file: file, line: line)
    }
    
    static func expectedAtLeastNRemainingBytes(_ expected: Int, actual: Int, file: String = #file, line: Int = #line) -> Self {
        return PSQLPartialDecodingError(
            description: "Expected at least '\(expected)' remaining bytes. But only found \(actual).",
            file: file, line: line)
    }
    
    static func expectedExactlyNRemainingBytes(_ expected: Int, actual: Int, file: String = #file, line: Int = #line) -> Self {
        return PSQLPartialDecodingError(
            description: "Expected exactly '\(expected)' remaining bytes. But found \(actual).",
            file: file, line: line)
    }
    
    static func fieldNotDecodable(type: Any.Type, file: String = #file, line: Int = #line) -> Self {
        return PSQLPartialDecodingError(
            description: "Could not read '\(type)' from ByteBuffer.",
            file: file, line: line)
    }
    
    static func integerMustBePositiveOrNull<Number: FixedWidthInteger>(_ actual: Number, file: String = #file, line: Int = #line) -> Self {
        return PSQLPartialDecodingError(
            description: "Expected the integer to be positive or null, but got \(actual).",
            file: file, line: line)
    }
}

extension ByteBuffer {
    mutating func throwingReadInteger<I: FixedWidthInteger>(as: I.Type, file: String = #file, line: Int = #line) throws -> I {
        guard let result = self.readInteger(endianness: .big, as: I.self) else {
            throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(MemoryLayout<I>.size, actual: self.readableBytes, file: file, line: line)
        }
        return result
    }
    
    mutating func throwingMoveReaderIndex(forwardBy offset: Int, file: String = #file, line: Int = #line) throws {
        guard self.readSlice(length: offset) != nil else {
            throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(offset, actual: self.readableBytes, file: file, line: line)
        }
    }
}

