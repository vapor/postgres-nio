import NIOCore
import Logging

@preconcurrency
public protocol PostgresDatabase: Sendable {
    var logger: Logger { get }
    var eventLoop: any EventLoop { get }
    func send(
        _ request: some PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void>

    func withConnection<T>(_ closure: @escaping (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T>
}

extension PostgresDatabase {
    public func logging(to logger: Logger) -> any PostgresDatabase {
        _PostgresDatabaseCustomLogger(database: self, logger: logger)
    }
}

private struct _PostgresDatabaseCustomLogger {
    let database: any PostgresDatabase
    let logger: Logger
}

extension _PostgresDatabaseCustomLogger: PostgresDatabase {
    var eventLoop: any EventLoop {
        self.database.eventLoop
    }
    
    func send(_ request: some PostgresRequest, logger: Logger) -> EventLoopFuture<Void> {
        self.database.send(request, logger: logger)
    }
    
    func withConnection<T>(_ closure: @escaping (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.database.withConnection(closure)
    }
}
