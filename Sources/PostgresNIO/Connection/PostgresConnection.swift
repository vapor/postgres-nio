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

    private var didClose: Bool

    public var isClosed: Bool {
        return !self.channel.isActive
    }
    
    init(channel: Channel, logger: Logger) {
        self.channel = channel
        self.logger = logger
        self.didClose = false
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard !self.didClose else {
            return self.eventLoop.makeSucceededFuture(())
        }
        self.didClose = true
        if !self.isClosed {
            return self.channel.close(mode: .all)
        } else {
            return self.eventLoop.makeSucceededFuture(())
        }
    }
    
    deinit {
        assert(self.didClose, "PostgresConnection deinitialized before being closed.")
    }
}
