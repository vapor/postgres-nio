import NIO
import Logging

extension PostgresDatabase {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return simpleQuery(string) { rows.append($0) }.map { rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let query = PostgresSimpleQuery(query: string, decoder: self.decoder, onRow: onRow)
        return self.send(query, logger: self.logger)
    }
}

// MARK: Private

private final class PostgresSimpleQuery: PostgresRequest {
    var query: String
    let decoder: PostgresDecoder
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    
    init(query: String, decoder: PostgresDecoder, onRow: @escaping (PostgresRow) throws -> ()) {
        self.query = query
        self.decoder = decoder
        self.onRow = onRow
    }
    
    func log(to logger: Logger) {
        logger.debug("\(self.query)")
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if case .error = message.identifier {
            // we should continue after errors
            return []
        }
        switch message.identifier {
        case .dataRow:
            let data = try PostgresMessage.DataRow(message: message)
            guard let rowLookupTable = self.rowLookupTable else { fatalError() }
            let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable, decoder: self.decoder)
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
        case .notice:
            return []
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
