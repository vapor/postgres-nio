struct PSQLBackendMessageDecoder: NIOSingleStepByteToMessageDecoder {
    typealias InboundOut = PSQLBackendMessage
    
    private(set) var hasAlreadyReceivedBytes: Bool
    
    init(hasAlreadyReceivedBytes: Bool = false) {
        self.hasAlreadyReceivedBytes = hasAlreadyReceivedBytes
    }
    
    mutating func decode(buffer: inout ByteBuffer) throws -> PSQLBackendMessage? {
        // make sure we have at least one byte to read
        guard buffer.readableBytes > 0 else {
            return nil
        }
        
        if !self.hasAlreadyReceivedBytes {
            // We have not received any bytes yet! Let's peek at the first message id. If it
            // is a "S" or "N" we assume that it is connected to an SSL upgrade request. All
            // other messages that we expect now, don't start with either "S" or "N"
            
            // we made sure, we have at least one byte available, above, thus force unwrap is okay
            let firstByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self)!
            
            switch firstByte {
            case UInt8(ascii: "S"):
                // mark byte as read
                buffer.moveReaderIndex(forwardBy: 1)
                self.hasAlreadyReceivedBytes = true
                return .sslSupported
            case UInt8(ascii: "N"):
                // mark byte as read
                buffer.moveReaderIndex(forwardBy: 1)
                self.hasAlreadyReceivedBytes = true
                return .sslUnsupported
            default:
                self.hasAlreadyReceivedBytes = true
            }
        }
        
        // all other packages have an Int32 after the identifier that determines their length.
        // do we have enough bytes for that?
        guard buffer.readableBytes >= 5 else {
            return nil
        }
        
        let idByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self)!
        let length = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self)!
        
        guard length + 1 <= buffer.readableBytes else {
            return nil
        }
        
        // At this point we are sure, that we have enough bytes to decode the next message.
        // 1. Create a byteBuffer that represents exactly the next message. This can be force
        //    unwrapped, since it was verified that enough bytes are available.
        let completeMessageBuffer = buffer.readSlice(length: 1 + Int(length))!
        
        // 2. make sure we have a known message identifier
        guard let messageID = PSQLBackendMessage.ID(rawValue: idByte) else {
            throw PSQLDecodingError.unknownMessageIDReceived(messageID: idByte, messageBytes: completeMessageBuffer)
        }
        
        // 3. decode the message
        do {
            // get a mutable byteBuffer copy
            var slice = completeMessageBuffer
            // move reader index forward by five bytes
            slice.moveReaderIndex(forwardBy: 5)
            
            return try PSQLBackendMessage.decode(from: &slice, for: messageID)
        } catch let error as PSQLPartialDecodingError {
            throw PSQLDecodingError.withPartialError(error, messageID: messageID.rawValue, messageBytes: completeMessageBuffer)
        } catch {
            preconditionFailure("Expected to only see `PartialDecodingError`s here.")
        }
    }
    
    mutating func decodeLast(buffer: inout ByteBuffer, seenEOF: Bool) throws -> PSQLBackendMessage? {
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
    func psqlEnsureAtLeastNBytesRemaining(_ n: Int, file: String = #file, line: Int = #line) throws {
        guard self.readableBytes >= n else {
            throw PSQLPartialDecodingError.expectedAtLeastNRemainingBytes(2, actual: self.readableBytes, file: file, line: line)
        }
    }

    func psqlEnsureExactNBytesRemaining(_ n: Int, file: String = #file, line: Int = #line) throws {
        guard self.readableBytes == n else {
            throw PSQLPartialDecodingError.expectedExactlyNRemainingBytes(n, actual: self.readableBytes, file: file, line: line)
        }
    }
}

