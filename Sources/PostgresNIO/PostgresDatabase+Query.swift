import NIOCore
import Logging
import NIOConcurrencyHelpers

extension PostgresDatabase {
    public func query(
        _ string: String,
        _ binds: [PostgresData] = []
    ) -> EventLoopFuture<PostgresQueryResult> {
        let box = NIOLockedValueBox((metadata: PostgresQueryMetadata?.none, rows: [PostgresRow]()))

        return self.query(string, binds, onMetadata: { metadata in
            box.withLockedValue {
                $0.metadata = metadata
            }
        }) { row in
            box.withLockedValue {
                $0.rows.append(row)
            }
        }.map {
            box.withLockedValue {
                PostgresQueryResult(metadata: $0.metadata!, rows: $0.rows)
            }
        }
    }

    @preconcurrency
    public func query(
        _ string: String,
        _ binds: [PostgresData] = [],
        onMetadata: @Sendable @escaping (PostgresQueryMetadata) -> () = { _ in },
        onRow: @Sendable @escaping (PostgresRow) throws -> ()
    ) -> EventLoopFuture<Void> {
        var bindings = PostgresBindings(capacity: binds.count)
        binds.forEach { bindings.append($0) }
        let query = PostgresQuery(unsafeSQL: string, binds: bindings)
        let request = PostgresCommands.query(query, onMetadata: onMetadata, onRow: onRow)
        
        return self.send(request, logger: logger)
    }
}

public struct PostgresQueryResult {
    public let metadata: PostgresQueryMetadata
    public let rows: [PostgresRow]
}

extension PostgresQueryResult: Collection {
    public typealias Index = Int
    public typealias Element = PostgresRow

    public var startIndex: Int {
        self.rows.startIndex
    }

    public var endIndex: Int {
        self.rows.endIndex
    }

    public subscript(position: Int) -> PostgresRow {
        self.rows[position]
    }

    public func index(after i: Int) -> Int {
        self.rows.index(after: i)
    }
}

public struct PostgresQueryMetadata: Sendable {
    public let command: String
    public var oid: Int?
    public var rows: Int?

    init?(string: String) {
        let parts = string.split(separator: " ")
        switch parts.first {
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
