import NIOCore
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
    
    /// A dictionary to store notification callbacks in
    ///
    /// Those are used when `PostgresConnection.addListener` is invoked. This only lives here since properties
    /// can not be added in extensions. All relevant code lives in `PostgresConnection+Notifications`
    var notificationListeners: [String: [(PostgresListenContext, (PostgresListenContext, PostgresMessage.NotificationResponse) -> Void)]] = [:] {
        willSet {
            self.underlying.channel.eventLoop.preconditionInEventLoop()
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
