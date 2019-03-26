import NIO

extension PostgresClient {
    public func query(_ string: String, _ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: [PostgresData] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let query = PostgresParameterizedQuery(query: string, binds: binds, onRow: onRow)
        return self.send(query)
    }
}

// MARK: Private

private final class PostgresParameterizedQuery: PostgresRequestHandler {
    let query: String
    let binds: [PostgresData]
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    var resultFormatCodes: [PostgresFormatCode]
    
    init(
        query: String,
        binds: [PostgresData],
        onRow: @escaping (PostgresRow) throws -> ()
    ) {
        self.query = query
        self.binds = binds
        self.onRow = onRow
        self.resultFormatCodes = [.binary]
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
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
            return []
        case .commandComplete:
            return []
        case .readyForQuery:
            return nil
        default: throw PostgresError.protocol("Unexpected message during query: \(message)")
        }
    }
    
    func start() throws -> [PostgresMessage] {
        let parse = PostgresMessage.Parse(
            statementName: "",
            query: self.query,
            parameterTypes: []
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
