import NIO

extension PostgresMessage {
    /// Encodes `PostgresMessage`s to outgoing data.
    final class ChannelEncoder: MessageToByteEncoder {
        /// See `MessageToByteEncoder`.
        typealias OutboundIn = PostgresMessage
        
        /// See `MessageToByteEncoder`.
        func encode(ctx: ChannelHandlerContext, data: PostgresMessage, out: inout ByteBuffer) throws {
            // print("PostgresMessage.ChannelEncoder.encode(\(data))")
            // serialize identifier
            switch data {
            case .password: out.write(identifier: .passwordMessage)
            case .simpleQuery: out.write(identifier: .query)
            default: break
            }
            
            // leave room for identifier and size
            let messageSizeIndex = out.writerIndex
            out.moveWriterIndex(forwardBy: 4)
            
            // serialize the message data
            switch data {
            case .startup(let startup): startup.serialize(into: &out)
            case .password(let password): password.serialize(into: &out)
            case .simpleQuery(let query): query.serialize(into: &out)
            default: fatalError("Unsupported outgoing message: \(data)")
            }
            
            // set message size
            out.set(integer: Int32(out.writerIndex - messageSizeIndex), at: messageSizeIndex)
        }
    }
}
