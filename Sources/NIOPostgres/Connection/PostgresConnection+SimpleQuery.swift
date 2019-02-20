import NIO

extension PostgresConnection {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return simpleQuery(string) { rows.append($0) }.map { rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let query = PostgresSimpleQuery(query: string, onRow: onRow)
        return self.send(query)
    }
}

// MARK: Private

private final class PostgresSimpleQuery: PostgresConnectionRequest {
    var query: String
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    
    init(query: String, onRow: @escaping (PostgresRow) throws -> ()) {
        self.query = query
        self.onRow = onRow
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        switch message.identifier {
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
                resultFormat: []
            )
            return []
        case .commandComplete:
            return []
        case .readyForQuery:
            return nil
        default:
            throw PostgresError.protocol("Unexpected message during simple query: \(message)")
        }
    }
    
    func start() throws -> [PostgresMessage] {
        return try [
            PostgresMessage.SimpleQuery(string: self.query).message()
        ]
    }
}
