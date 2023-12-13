import NIOCore
import Logging
import NIOConcurrencyHelpers

public struct PostgresQueryMetadata: Sendable {
    public let command: String
    public var oid: Int?
    public var rows: Int?

    init?(string: String) {
        let parts = string.split(separator: " ")
        guard parts.count >= 1 else {
            return nil
        }
        switch parts[0] {
        case "INSERT":
            // INSERT oid rows
            guard parts.count == 3 else {
                return nil
            }
            self.command = .init(parts[0])
            self.oid = Int(parts[1])
            self.rows = Int(parts[2])
        case "SELECT" where parts.count == 1:
            // AWS Redshift does not return the actual row count as defined in the postgres wire spec for SELECT:
            // https://www.postgresql.org/docs/13/protocol-message-formats.html in section `CommandComplete`
            self.command = "SELECT"
            self.oid = nil
            self.rows = nil
        case "SELECT", "DELETE", "UPDATE", "MOVE", "FETCH", "COPY":
            // <cmd> rows
            guard parts.count == 2 else {
                return nil
            }
            self.command = .init(parts[0])
            self.oid = nil
            self.rows = Int(parts[1])
        default:
            // <cmd>
            self.command = string
            self.oid = nil
            self.rows = nil
        }
    }
}
