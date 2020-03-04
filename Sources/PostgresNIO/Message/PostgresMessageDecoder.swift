import NIO

public final class PostgresMessageDecoder: ByteToMessageDecoder {
    /// See `ByteToMessageDecoder`.
    public typealias InboundOut = PostgresMessage
    
    /// See `ByteToMessageDecoder`.
    public var cumulationBuffer: ByteBuffer?
    
    /// If `true`, the server has asked for authentication.
    public var hasSeenFirstMessage: Bool

    /// Logger to send debug messages to.
    let logger: Logger?
    
    /// Creates a new `PostgresMessageDecoder`.
    public init(logger: Logger? = nil) {
        self.hasSeenFirstMessage = false
        self.logger = logger
    }
    
    /// See `ByteToMessageDecoder`.
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        var peekBuffer = buffer
        
        // peek at the message identifier
        // the message identifier is always the first byte of a message
        guard let identifier = peekBuffer.readInteger(as: UInt8.self).map(PostgresMessage.Identifier.init) else {
            return .needMoreData
        }

        let message: PostgresMessage
        
        // special ssl case, no body
        if !self.hasSeenFirstMessage && (identifier == .sslSupported || identifier == .sslUnsupported) {
            message = PostgresMessage(identifier: identifier, data: context.channel.allocator.buffer(capacity: 0))
        } else {
            // peek at the message size
            // the message size is always a 4 byte integer appearing immediately after the message identifier
            guard let messageSize = peekBuffer.readInteger(as: Int32.self).flatMap(Int.init) else {
                return .needMoreData
            }
            
            // ensure message is large enough (skipping message type) or reject
            guard let data = peekBuffer.readSlice(length: messageSize - 4) else {
                return .needMoreData
            }
            
            message = PostgresMessage(identifier: identifier, data: data)
        }
        self.hasSeenFirstMessage = true
        
        // there is sufficient data, use this buffer
        buffer = peekBuffer
        self.logger?.trace("Decoded: PostgresMessage (\(message.identifier))")
        context.fireChannelRead(wrapInboundOut(message))
        return .continue
    }
    
    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        // ignore
        return .needMoreData
    }
}
