import Foundation

public struct PostgresError: Error, LocalizedError, CustomStringConvertible {
    enum Reason {
        case `protocol`(String)
        case server(PostgresMessage.Error)
    }
    
    let reason: Reason
    
    /// See `LocalizedError`.
    public var errorDescription: String? {
        return description
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        let description: String
        switch reason {
        case .protocol(let message): description = "protocol error: \(message)"
        case .server(let error):
            let severity = error.fields[.severity] ?? "ERROR"
            let unique = error.fields[.routine] ?? error.fields[.sqlState] ?? "unknown"
            let message = error.fields[.message] ?? "Unknown"
            description = "server \(severity.lowercased()): \(message) (\(unique))"
        }
        return "NIOPostgres \(description)"
    }
    
    init(_ reason: Reason) {
        self.reason = reason
    }
}
