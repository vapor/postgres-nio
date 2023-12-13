import Foundation

public enum PostgresError: Error, LocalizedError, CustomStringConvertible {
    case `protocol`(String)
    case connectionClosed
    
    /// See `LocalizedError`.
    public var errorDescription: String? {
        return self.description
    }
    
    /// See `CustomStringConvertible`.
    public var description: String {
        let description: String
        switch self {
        case .protocol(let message):
            description = "protocol error: \(message)"
        case .connectionClosed:
            description = "connection closed"
        }
        return "NIOPostgres error: \(description)"
    }
}
