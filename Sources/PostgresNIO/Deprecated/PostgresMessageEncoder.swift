import NIOCore
import Logging

@available(*, deprecated, message: "Will be removed from public API")
public final class PostgresMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = PostgresMessage

    /// Logger to send debug messages to.
    let logger: Logger?

    /// Creates a new `PostgresMessageEncoder`.
    public init(logger: Logger? = nil) {
        self.logger = logger
    }
    
    /// See `MessageToByteEncoder`.
    public func encode(data message: PostgresMessage, out: inout ByteBuffer) throws {
        // serialize identifier
        var message = message
        switch message.identifier {
        case .none: break
        default:
            out.write(identifier: message.identifier)
        }
        
        // leave room for identifier and size
        let messageSizeIndex = out.writerIndex
        out.moveWriterIndex(forwardBy: 4)
        
        // serialize the message data
        out.writeBuffer(&message.data)
        
        // set message size
        out.setInteger(Int32(out.writerIndex - messageSizeIndex), at: messageSizeIndex)
        self.logger?.trace("Encoded: PostgresMessage (\(message.identifier))")
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}
