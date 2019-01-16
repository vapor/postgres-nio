import NIO

public final class PostgresMessageEncoder: MessageToByteEncoder {
    /// See `MessageToByteEncoder`.
    public typealias OutboundIn = PostgresMessage
    
    /// See `MessageToByteEncoder`.
    public func encode(ctx: ChannelHandlerContext, data message: PostgresMessage, out: inout ByteBuffer) throws {
        // print("PostgresMessage.ChannelEncoder.encode(\(data))")
        
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
        out.write(buffer: &message.data)
        
        // set message size
        out.set(integer: Int32(out.writerIndex - messageSizeIndex), at: messageSizeIndex)
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}
