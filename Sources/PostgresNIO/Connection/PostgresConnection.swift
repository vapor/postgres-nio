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

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.isClosed else {
            return self.eventLoop.makeSucceededFuture(())
        }
        return self.channel.close(mode: .all)
    }
    
    deinit {
        assert(self.isClosed, "PostgresConnection deinitialized before being closed.")
    }
}
