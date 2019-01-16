public final class PostgresConnection {
//    #warning("publicize these values?")
//    public var status: [String: String]
//    var processID: Int32?
//    var secretKey: Int32?
    let channel: Channel
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    init(channel: Channel) {
        self.channel = channel
    }
    
    public func close() -> EventLoopFuture<Void> {
        return self.channel.close(mode: .all)
    }
}

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
            ctx.fireErrorCaught(error)
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
