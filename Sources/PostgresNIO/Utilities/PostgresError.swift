import Foundation

public enum PostgresError: Error, LocalizedError, CustomStringConvertible {
    case `protocol`(String)
    case server(PostgresMessage.Error)
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
        case .server(let error):
            return "server: \(error.description)"
        case .connectionClosed:
            description = "connection closed"
        }
        return "NIOPostgres error: \(description)"
    }
}
