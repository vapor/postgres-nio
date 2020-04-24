import NIO
import Logging

extension PostgresDatabase {
    public func query(
        _ string: String,
        _ binds: [PostgresData] = []
    ) -> EventLoopFuture<PostgresQueryResult> {
        var rows: [PostgresRow] = []
        var metadata: PostgresQueryMetadata?
        return self.query(string, binds, onMetadata: {
            metadata = $0
        }) {
            rows.append($0)
        }.map {
            .init(metadata: metadata!, rows: rows)
        }
    }

    public func query(
        _ string: String,
        _ binds: [PostgresData] = [],
        onMetadata: @escaping (PostgresQueryMetadata) -> () = { _ in },
        onRow: @escaping (PostgresRow) throws -> ()
    ) -> EventLoopFuture<Void> {
        let query = PostgresParameterizedQuery(
            query: string,
            binds: binds,
            onMetadata: onMetadata,
            onRow: onRow
        )
        return self.send(query, logger: self.logger)
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

public struct PostgresQueryMetadata {
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
        case "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY":
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

// MARK: Private

private final class PostgresParameterizedQuery: PostgresRequest {
    let query: String
    let binds: [PostgresData]
    var onMetadata: (PostgresQueryMetadata) -> ()
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    var resultFormatCodes: [PostgresFormatCode]
    var logger: Logger?

    init(
        query: String,
        binds: [PostgresData],
        onMetadata: @escaping (PostgresQueryMetadata) -> (),
        onRow: @escaping (PostgresRow) throws -> ()
    ) {
        self.query = query
        self.binds = binds
        self.onMetadata = onMetadata
        self.onRow = onRow
        self.resultFormatCodes = [.binary]
    }

    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("\(self.query) \(self.binds)")
    }

    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if case .error = message.identifier {
            // we should continue after errors
            return []
        }
        switch message.identifier {
        case .bindComplete:
            return []
        case .dataRow:
            let data = try PostgresMessage.DataRow(message: message)
            guard let rowLookupTable = self.rowLookupTable else { fatalError() }
            let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
            try onRow(row)
            return []
        case .rowDescription:
            let row = try PostgresMessage.RowDescription(message: message)
            self.rowLookupTable = PostgresRow.LookupTable(
                rowDescription: row,
                resultFormat: self.resultFormatCodes
            )
            return []
        case .noData:
            return []
        case .parseComplete:
            return []
        case .parameterDescription:
            let params = try PostgresMessage.ParameterDescription(message: message)
            if params.dataTypes.count != self.binds.count {
                self.logger!.warning("Expected parameters count (\(params.dataTypes.count)) does not equal binds count (\(binds.count))")
            } else {
                for (i, item) in zip(params.dataTypes, self.binds).enumerated() {
                    if item.0 != item.1.type {
                        self.logger!.warning("bind $\(i + 1) type (\(item.1.type)) does not match expected parameter type (\(item.0))")
                    }
                }
            }
            return []
        case .commandComplete:
            let complete = try PostgresMessage.CommandComplete(message: message)
            guard let metadata = PostgresQueryMetadata(string: complete.tag) else {
                throw PostgresError.protocol("Unexpected query metadata: \(complete.tag)")
            }
            self.onMetadata(metadata)
            return []
        case .notice:
            return []
        case .notificationResponse:
            return []
        case .readyForQuery:
            return nil
        default: throw PostgresError.protocol("Unexpected message during query: \(message)")
        }
    }

    func start() throws -> [PostgresMessage] {
        guard self.binds.count <= Int16.max else {
            throw PostgresError.protocol("Bind count must be <= \(Int16.max).")
        }
        let parse = PostgresMessage.Parse(
            statementName: "",
            query: self.query,
            parameterTypes: self.binds.map { $0.type }
        )
        let describe = PostgresMessage.Describe(
            command: .statement,
            name: ""
        )
        let bind = PostgresMessage.Bind(
            portalName: "",
            statementName: "",
            parameterFormatCodes: self.binds.map { $0.formatCode },
            parameters: self.binds.map { .init(value: $0.value) },
            resultFormatCodes: self.resultFormatCodes
        )
        let execute = PostgresMessage.Execute(
            portalName: "",
            maxRows: 0
        )

        let sync = PostgresMessage.Sync()
        return try [parse.message(), describe.message(), bind.message(), execute.message(), sync.message()]
    }
}
