import NIO

extension PostgresMessage {
    /// Encodes `PostgresMessage`s to outgoing data.
    final class OutboundHandler: MessageToByteEncoder {
        /// See `MessageToByteEncoder`.
        typealias OutboundIn = PostgresMessage
        
        /// See `MessageToByteEncoder`.
        func encode(ctx: ChannelHandlerContext, data: PostgresMessage, out: inout ByteBuffer) throws {
            // print("PostgresMessage.ChannelEncoder.encode(\(data))")
            
            // serialize identifier + set packet
            let packet: ByteBufferSerializable?
            switch data {
            case .bind(let bind):
                out.write(identifier: .bind)
                packet = bind
            case .describe(let describe):
                out.write(identifier: .describe)
                packet = describe
            case .execute(let execute):
                out.write(identifier: .execute)
                packet = execute
            case .parse(let parse):
                out.write(identifier: .parse)
                packet = parse
            case .password(let password):
                out.write(identifier: .passwordMessage)
                packet = password
            case .simpleQuery(let query):
                out.write(identifier: .query)
                packet = query
            case .startup(let startup):
                packet = startup
            case .sync:
                out.write(identifier: .sync)
                packet = nil
            default: fatalError("Unsupported outgoing message: \(data)")
            }
            
            // leave room for identifier and size
            let messageSizeIndex = out.writerIndex
            out.moveWriterIndex(forwardBy: 4)
            
            // serialize the message data
            packet?.serialize(into: &out)
            
            // set message size
            out.set(integer: Int32(out.writerIndex - messageSizeIndex), at: messageSizeIndex)
        }
    }
}

protocol ByteBufferSerializable {
    func serialize(into buffer: inout ByteBuffer)
}
