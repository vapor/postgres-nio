public protocol PostgresDatabase {
    var eventLoop: EventLoop { get }
    func send(_ request: PostgresRequestHandler) -> EventLoopFuture<Void>
}

extension PostgresConnection: PostgresDatabase { }
