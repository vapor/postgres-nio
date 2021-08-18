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
        try .makeAddressResolvingHost(env("POSTGRES_HOSTNAME") ?? "localhost", port: 5432)
    }

    static func testUnauthenticated(on eventLoop: EventLoop, logLevel: Logger.Level = .info) -> EventLoopFuture<PostgresConnection> {
        var logger = Logger(label: "postgres.connection.test")
        logger.logLevel = logLevel
        do {
            return connect(to: try address(), logger: logger, on: eventLoop)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }

    static func test(on eventLoop: EventLoop, logLevel: Logger.Level = .info) -> EventLoopFuture<PostgresConnection> {
        return testUnauthenticated(on: eventLoop, logLevel: logLevel).flatMap { conn in
            return conn.authenticate(
                username: env("POSTGRES_USER") ?? "vapor_username",
                database: env("POSTGRES_DB") ?? "vapor_database",
                password: env("POSTGRES_PASSWORD") ?? "vapor_password"
            ).map {
                return conn
            }.flatMapError { error in
                conn.close().flatMapThrowing {
                    throw error
                }
            }
        }
    }
}

extension Logger {
    static var psqlTest: Logger {
        var logger = Logger(label: "psql.test")
        logger.logLevel = .info
        return logger
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
