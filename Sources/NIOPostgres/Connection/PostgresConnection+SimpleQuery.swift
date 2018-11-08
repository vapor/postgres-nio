import NIO

extension PostgresConnection {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return simpleQuery(string) { rows.append($0) }.map { rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) -> ()) -> EventLoopFuture<Void> {
        var rowLookupTable: PostgresRow.LookupTable?
        return handler.send([.simpleQuery(.init(string: string))]) { message in
            switch message {
            case .dataRow(let data):
                guard let rowLookupTable = rowLookupTable else { fatalError() }
                let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
                onRow(row)
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
            case .readyForQuery:
                return true
            default: throw PostgresError(.protocol("Unexpected message during simple query: \(message)"))
            }
        }
    }
}
