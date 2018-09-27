import NIO

public final class PostgresConnection {
    let handler: ChannelHandler
    
    #warning("publicize these values?")
    var status: [String: String]
    var processID: Int32?
    var secretKey: Int32?
    
    public var closeFuture: EventLoopFuture<Void> {
        return handler.channel.closeFuture
    }
    
    init(_ handler: ChannelHandler) {
        self.handler = handler
        self.status = [:]
    }
    
    public func close() -> EventLoopFuture<Void> {
        return handler.channel.close(mode: .all)
    }
}
