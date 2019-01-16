protocol PostgresConnectionHandler: ChannelInboundHandler where
    InboundIn == PostgresMessage,
    OutboundOut == PostgresMessage
{
    func read(message: inout PostgresMessage, ctx: ChannelHandlerContext) throws
}

extension PostgresConnectionHandler {
    func channelRead(ctx: ChannelHandlerContext, data: NIOAny) {
        var message = self.unwrapInboundIn(data)
        do {
            try self.read(message: &message, ctx: ctx)
        } catch {
            self.errorCaught(ctx: ctx, error: error)
        }
    }
}

extension ChannelHandlerContext {
    func write(message type: PostgresMessageType, promise: EventLoopPromise<Void>?) {
        do {
            var message = PostgresMessage(identifier: .none, data: self.channel.allocator.buffer(capacity: 0))
            try type.serialize(to: &message)
            self.write(NIOAny(message), promise: promise)
        } catch {
            self.fireErrorCaught(error)
        }
    }
}
