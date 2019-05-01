import Foundation
import Logging

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
    
    public var logger: Logger
    
    init(channel: Channel) {
        self.channel = channel
        self.logger = Logger(label: "codes.vapor.nio-postgres")
    }
    
    public func close() -> EventLoopFuture<Void> {
        guard self.channel.isActive else {
            return self.eventLoop.makeSucceededFuture(())
        }
        return self.channel.close()
    }
    
    #warning("TODO: add error handler that closes connection")
    deinit {
        if self.channel.isActive {
            assertionFailure("PostgresConnection deinitialized before being closed.")
        }
    }
}
