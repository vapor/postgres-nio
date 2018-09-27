import NIO

/// Decodes `PostgresMessage`s from incoming data.
final class PostgresMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    var cumulationBuffer: ByteBuffer?
    
    /// If `true`, the server has asked for authentication.
    var hasRequestedAuthentication: Bool
    
    /// Creates a new `PostgresMessageDecoder`.
    init() {
        self.hasRequestedAuthentication = false
    }
    
    /// See `ByteToMessageDecoder`.
    func decode(ctx: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        // peek at the message identifier
        // the message identifier is always the first byte of a message
        guard let messageIdentifier = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self).map(PostgresMessage.Identifier.init) else {
            return .needMoreData
        }
        
        #warning("check for TLS support")
        
        // peek at the message size
        // the message size is always a 4 byte integer appearing immediately after the message identifier
        guard let messageSize = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self).flatMap(Int.init) else {
            return .needMoreData
        }
        
        // ensure message is large enough (skipping message type) or reject
        guard buffer.readableBytes - 1 >= messageSize else {
            return .needMoreData
        }
        
        // skip message identifier and message size
        buffer.moveReaderIndex(forwardBy: 1 + 4)
    }
}
