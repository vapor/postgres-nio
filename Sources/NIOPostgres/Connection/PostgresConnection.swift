import Foundation
import Logging

public final class PostgresConnection {
    let channel: Channel
    
    public var eventLoop: EventLoop {
        return self.channel.eventLoop
    }
    
    public var closeFuture: EventLoopFuture<Void> {
        return channel.closeFuture
    }
    
    public var logger: Logger
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard self.channel.isActive else {
            return self.eventLoop.makeSucceededFuture(())
        }
        return self.channel.close()
    }
    
    deinit {
        if self.channel.isActive {
            assertionFailure("PostgresConnection deinitialized before being closed.")
        }
    }
}
