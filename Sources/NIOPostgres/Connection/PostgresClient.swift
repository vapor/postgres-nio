public protocol PostgresClient {
    var eventLoop: EventLoop { get }
    func send(_ request: PostgresRequest) -> EventLoopFuture<Void>
}
