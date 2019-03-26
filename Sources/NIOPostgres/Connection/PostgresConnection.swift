import Foundation
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
    
    #warning("TODO: add error handler that closes connection")
    deinit {
        if self.channel.isActive {
            assertionFailure("PostgresConnection deinitialized before being closed.")
        }
    }
}
