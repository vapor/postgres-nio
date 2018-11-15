import NIO

extension PostgresConnection {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return simpleQuery(string) { rows.append($0) }.map { rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        var error: PostgresMessage.Error?
        var rowLookupTable: PostgresRow.LookupTable?
        return handler.send([.simpleQuery(.init(string: string))]) { message in
            switch message {
            case .dataRow(let data):
                guard let rowLookupTable = rowLookupTable else { fatalError() }
                let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
                try onRow(row)
                return false
            case .rowDescription(let r):
                rowLookupTable = PostgresRow.LookupTable(
                    rowDescription: r,
                    tableNames: self.tableNames,
                    resultFormat: []
                )
                return false
            case .commandComplete(let complete):
                return false
            case .error(let e):
                error = e
                return false
            case .notice(let notice):
                print("[NIOPostgres] [NOTICE] \(notice)")
                return false
            case .readyForQuery:
                if let error = error {
                    throw PostgresError(.server(error))
                }
                return true
            default: throw PostgresError(.protocol("Unexpected message during simple query: \(message)"))
            }
        }
    }
}
