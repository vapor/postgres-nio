#warning("TODO: make this protocol more general, not depend on PostgresRequestHandler")
public protocol PostgresClient {
    var eventLoop: EventLoop { get }
    func send(_ request: PostgresRequestHandler) -> EventLoopFuture<Void>
}

extension PostgresConnection: PostgresClient { }
