import XCTest
import PostgresNIO
import NIOCore
import Logging

extension PostgresConnection {
    static func address() throws -> SocketAddress {
        try .makeAddressResolvingHost(TestConfiguration.hostname, port: TestConfiguration.port)
    }
    
    @available(*, deprecated, message: "Test deprecated functionality")
    static func testUnauthenticated(on eventLoop: any EventLoop, logLevel: Logger.Level = .info) -> EventLoopFuture<PostgresConnection> {
        var logger = Logger(label: "postgres.connection.test")
        logger.logLevel = logLevel
        do {
            return connect(to: try address(), logger: logger, on: eventLoop)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }

    static func test(on eventLoop: any EventLoop, options: Configuration.Options? = nil) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        var config = PostgresConnection.Configuration(
            host: TestConfiguration.hostname,
            port: TestConfiguration.port,
            username: TestConfiguration.username,
            password: TestConfiguration.password,
            database: TestConfiguration.database,
            tls: .disable
        )
        if let options {
            config.options = options
        }
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
    
    static func testUDS(on eventLoop: any EventLoop) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        let config = PostgresConnection.Configuration(
            unixSocketPath: TestConfiguration.defaultUnixSocketPath,
            username: TestConfiguration.username,
            password: TestConfiguration.password,
            database: TestConfiguration.database
        )
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
    
    static func testChannel(_ channel: any Channel, on eventLoop: any EventLoop) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        let config = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: TestConfiguration.username,
            password: TestConfiguration.password,
            database: TestConfiguration.database
        )
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
}

extension Logger {
    static var psqlTest: Logger {
        .init(label: "psql.test")
    }
}

extension XCTestCase {
    
    public static var shouldRunLongRunningTests: Bool {
        TestConfiguration.shouldRunLongRunningTests
    }
    
    public static var shouldRunPerformanceTests: Bool {
        TestConfiguration.shouldRunPerformanceTests
    }
}
