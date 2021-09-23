import NIOCore

/// This is a reverse ``NIOCore/ByteToMessageHandler``. Instead of creating messages from incoming bytes
/// as the normal `ByteToMessageHandler` does, this `ReverseByteToMessageHandler` creates messages
/// from outgoing bytes. This is only important for testing in `EmbeddedChannel`s.
class ReverseMessageToByteHandler<Encoder: MessageToByteEncoder>: ChannelInboundHandler {
    typealias InboundIn = Encoder.OutboundIn
    typealias InboundOut = ByteBuffer
    
    var byteBuffer: ByteBuffer!
    let encoder: Encoder
    
    init(_ encoder: Encoder) {
        self.encoder = encoder
    }
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.byteBuffer = context.channel.allocator.buffer(capacity: 128)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let message = self.unwrapInboundIn(data)
        
        do {
            self.byteBuffer.clear()
            try self.encoder.encode(data: message, out: &self.byteBuffer)
            context.fireChannelRead(self.wrapInboundOut(self.byteBuffer))
        } catch {
            context.fireErrorCaught(error)
        }
    }
}
