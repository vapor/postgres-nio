import XCTest
import PostgresNIO
import NIOCore
import Logging
#if canImport(Darwin)
import Darwin.C
#else
import Glibc
#endif

extension PostgresConnection {
    static func address() throws -> SocketAddress {
        try .makeAddressResolvingHost(env("POSTGRES_HOSTNAME") ?? "localhost", port: env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 5432)
    }
    
    @available(*, deprecated, message: "Test deprecated functionality")
    static func testUnauthenticated(on eventLoop: EventLoop, logLevel: Logger.Level = .info) -> EventLoopFuture<PostgresConnection> {
        var logger = Logger(label: "postgres.connection.test")
        logger.logLevel = logLevel
        do {
            return connect(to: try address(), logger: logger, on: eventLoop)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }

    static func test(on eventLoop: EventLoop, options: Configuration.Options? = nil) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        var config = PostgresConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 5432,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database",
            tls: .disable
        )
        if let options {
            config.options = options
        }
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
    
    static func testUDS(on eventLoop: EventLoop) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        let config = PostgresConnection.Configuration(
            unixSocketPath: env("POSTGRES_SOCKET") ?? "/tmp/.s.PGSQL.\(env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 5432)",
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database"
        )
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
    
    static func testChannel(_ channel: Channel, on eventLoop: EventLoop) -> EventLoopFuture<PostgresConnection> {
        let logger = Logger(label: "postgres.connection.test")
        let config = PostgresConnection.Configuration(
            establishedChannel: channel,
            username: env("POSTGRES_USER") ?? "test_username",
            password: env("POSTGRES_PASSWORD") ?? "test_password",
            database: env("POSTGRES_DB") ?? "test_database"
        )
        
        return PostgresConnection.connect(on: eventLoop, configuration: config, id: 0, logger: logger)
    }
}

extension Logger {
    static var psqlTest: Logger {
        .init(label: "psql.test")
    }
}

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

extension XCTestCase {
    
    public static var shouldRunLongRunningTests: Bool {
        // The env var must be set and have the value `"true"`, `"1"`, or `"yes"` (case-insensitive).
        // For the sake of sheer annoying pedantry, values like `"2"` are treated as false.
        guard let rawValue = env("POSTGRES_LONG_RUNNING_TESTS") else { return false }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }
    
    public static var shouldRunPerformanceTests: Bool {
        // Same semantics as above. Any present non-truthy value will explicitly disable performance
        // tests even if they would've overwise run in the current configuration.
        let defaultValue = !_isDebugAssertConfiguration() // default to not running in debug builds

        guard let rawValue = env("POSTGRES_PERFORMANCE_TESTS") else { return defaultValue }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }
}
