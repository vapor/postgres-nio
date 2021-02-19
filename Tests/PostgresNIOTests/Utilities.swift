import Logging
import PostgresNIO
import XCTest

extension PostgresConnection {
    static func address() throws -> SocketAddress {
        try .makeAddressResolvingHost( env("POSTGRES_HOSTNAME") ?? "localhost", port: 5432)
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

    static func test(on eventLoop: EventLoop, logLevel: Logger.Level = .trace) -> EventLoopFuture<PostgresConnection> {
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

extension XCTestCase {
    
    public static var shouldRunLongRunningTests: Bool {
        // The env var must be set and have the value `"true"`, `"1"`, or `"yes"` (case-insensitive).
        // For the sake of sheer annoying pedantry, values like `"2"` are treated as false.
        guard let rawValue = ProcessInfo.processInfo.environment["POSTGRES_LONG_RUNNING_TESTS"] else { return false }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }
    
    public static var shouldRunPerformanceTests: Bool {
        // Same semantics as above. Any present non-truthy value will explicitly disable performance
        // tests even if they would've overwise run in the current configuration.
        let defaultValue = !_isDebugAssertConfiguration() // default to not running in debug builds

        guard let rawValue = ProcessInfo.processInfo.environment["POSTGRES_PERFORMANCE_TESTS"] else { return defaultValue }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }
    
}


// 1247.typisdefined: 0x01 (BOOLEAN)
// 1247.typbasetype: 0x00000000 (OID)
// 1247.typnotnull: 0x00 (BOOLEAN)
// 1247.typcategory: 0x42 (CHAR)
// 1247.typname: 0x626f6f6c (NAME)
// 1247.typbyval: 0x01 (BOOLEAN)
// 1247.typrelid: 0x00000000 (OID)
// 1247.typalign: 0x63 (CHAR)
// 1247.typndims: 0x00000000 (INTEGER)
// 1247.typacl: null
// 1247.typsend: 0x00000985 (REGPROC)
// 1247.typmodout: 0x00000000 (REGPROC)
// 1247.typstorage: 0x70 (CHAR)
// 1247.typispreferred: 0x01 (BOOLEAN)
// 1247.typinput: 0x000004da (REGPROC)
// 1247.typoutput: 0x000004db (REGPROC)
// 1247.typlen: 0x0001 (SMALLINT)
// 1247.typcollation: 0x00000000 (OID)
// 1247.typdefaultbin: null
// 1247.typelem: 0x00000000 (OID)
// 1247.typnamespace: 0x0000000b (OID)
// 1247.typtype: 0x62 (CHAR)
// 1247.typowner: 0x0000000a (OID)
// 1247.typdefault: null
// 1247.typtypmod: 0xffffffff (INTEGER)
// 1247.typarray: 0x000003e8 (OID)
// 1247.typreceive: 0x00000984 (REGPROC)
// 1247.typmodin: 0x00000000 (REGPROC)
// 1247.typanalyze: 0x00000000 (REGPROC)
// 1247.typdelim: 0x2c (CHAR)
struct PGType: Decodable {
    var typname: String
    var typnamespace: UInt32
    var typowner: UInt32
    var typlen: Int16
}
