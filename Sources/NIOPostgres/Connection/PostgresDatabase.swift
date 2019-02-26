#warning("TODO: make this protocol more general, not depend on PostgresRequestHandler")
public protocol PostgresDatabase {
    var eventLoop: EventLoop { get }
    func send(_ request: PostgresRequestHandler) -> EventLoopFuture<Void>
}

extension PostgresConnection: PostgresDatabase { }
