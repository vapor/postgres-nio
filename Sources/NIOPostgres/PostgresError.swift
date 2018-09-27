import Foundation

public struct PostgresError: Error, LocalizedError {
    enum Reason {
        case `protocol`(String)
        case server(PostgresMessage.Error)
    }
    
    let reason: Reason
    
    public let file: String
    
    public let line: Int
    
    public let column: Int
    
    public let function: String
    
    /// See `LocalizedError`.
    public var errorDescription: String? {
        let description: String
        switch reason {
        case .protocol(let message): description = "protocol error: \(message)"
        case .server(let error):
            let severity = error.fields[.severity] ?? "ERROR"
            let unique = error.fields[.routine] ?? error.fields[.sqlState] ?? "unknown"
            let message = error.fields[.message] ?? "Unknown"
            description = "server \(severity): \(message) (\(unique))"
        }
        return "NIOPostgres \(description)"
    }
    
    init(
        _ reason: Reason,
        file: String = #file,
        line: Int = #line,
        column: Int = #column,
        function: String = #function
    ) {
        self.reason = reason
        self.file = file
        self.line = line
        self.column = column
        self.function = function
    }
}
