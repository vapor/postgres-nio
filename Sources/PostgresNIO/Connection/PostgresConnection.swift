import NIO
import Logging
import struct Foundation.UUID

public final class PostgresConnection {
    let underlying: PSQLConnection
    
    public var eventLoop: EventLoop {
        return self.underlying.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return self.underlying.channel.closeFuture
    }
    
    /// A logger to use in case 
    public var logger: Logger
    
    /// 
    var notificationListeners: [String: [(PostgresListenContext, (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)]] = [:] {
        didSet {
            self.underlying.channel.eventLoop.assertInEventLoop()
        }
    }

    public var isClosed: Bool {
        return !self.underlying.channel.isActive
    }
    
    init(underlying: PSQLConnection, logger: Logger) {
        self.underlying = underlying
        self.logger = logger
        
        self.underlying.channel.pipeline.handler(type: PSQLChannelHandler.self).whenSuccess { handler in
            handler.notificationDelegate = self
        }
    }
    
    public func close() -> EventLoopFuture<Void> {
        return self.underlying.close()
    }
}
