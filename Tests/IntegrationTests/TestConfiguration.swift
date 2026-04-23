#if canImport(Darwin)
import Darwin.C
#else
import Glibc
#endif

/// Centralized test configuration for all Postgres integration tests.
///
/// All connection parameters are resolved once from environment variables,
/// falling back to sensible defaults. Every test helper and factory should
/// read from this struct instead of duplicating `env(…) ?? "…"` calls.
enum TestConfiguration {
    // MARK: - Environment helper

    static func env(_ name: String) -> String? {
        getenv(name).flatMap { String(cString: $0) }
    }

    // MARK: - Connection parameters

    static var hostname: String {
        env("POSTGRES_HOSTNAME") ?? "127.0.0.1"
    }

    static var port: Int {
        env("POSTGRES_PORT").flatMap(Int.init(_:)) ?? 55432
    }

    static var username: String {
        env("POSTGRES_USER") ?? "test_username"
    }

    static var password: String {
        env("POSTGRES_PASSWORD") ?? "test_password"
    }

    static var database: String {
        env("POSTGRES_DB") ?? "test_database"
    }

    static var socket: String? {
        env("POSTGRES_SOCKET")
    }

    static var defaultUnixSocketPath: String {
        socket ?? "/tmp/.s.PGSQL.\(port)"
    }

    // MARK: - Auth method

    static var hostAuthMethod: String? {
        env("POSTGRES_HOST_AUTH_METHOD")
    }

    // MARK: - Test gates

    static var shouldRunLongRunningTests: Bool {
        guard let rawValue = env("POSTGRES_LONG_RUNNING_TESTS") else { return false }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }

    static var shouldRunPerformanceTests: Bool {
        let defaultValue = !_isDebugAssertConfiguration()
        guard let rawValue = env("POSTGRES_PERFORMANCE_TESTS") else { return defaultValue }
        if let boolValue = Bool(rawValue) { return boolValue }
        if let intValue = Int(rawValue) { return intValue == 1 }
        return rawValue.lowercased() == "yes"
    }
}
