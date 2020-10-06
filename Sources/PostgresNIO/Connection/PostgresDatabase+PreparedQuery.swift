import Foundation

extension PostgresDatabase {
    public func prepare(query: String) -> EventLoopFuture<PreparedQuery> {
        let name = "nio-postgres-\(UUID().uuidString)"
        let prepare = PrepareQueryRequest(query, as: name)
        return self.send(prepare, logger: self.logger).map { () -> (PreparedQuery) in
            let prepared = PreparedQuery(database: self, name: name, rowDescription: prepare.rowLookupTable)
            return prepared
        }
    }

    public func prepare(query: String, handler: @escaping (PreparedQuery) -> EventLoopFuture<[[PostgresRow]]>) -> EventLoopFuture<[[PostgresRow]]> {
        prepare(query: query)
        .flatMap { preparedQuery in
            handler(preparedQuery)
            .flatMap { results in
                preparedQuery.deallocate().map { results }
            }
        }
    }
}


public struct PreparedQuery {
    let database: PostgresDatabase
    let name: String
    let rowLookupTable: PostgresRow.LookupTable?

    init(database: PostgresDatabase, name: String, rowDescription: PostgresRow.LookupTable?) {
        self.database = database
        self.name = name
        self.rowLookupTable = rowDescription
    }

    public func execute(_ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return self.execute(binds) { rows.append($0) }.map { rows }
    }

    public func execute(_ binds: [PostgresData] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let handler = ExecutePreparedQuery(query: self, binds: binds, onRow: onRow)
        return database.send(handler, logger: database.logger)
    }

    public func deallocate() -> EventLoopFuture<Void> {
        database.send(CloseRequest(name: self.name,
                                   closeType: .preparedStatement),
                                   logger:database.logger)

    }
}


private final class PrepareQueryRequest: PostgresRequest {
    let query: String
    let name: String
    var rowLookupTable: PostgresRow.LookupTable?
    var resultFormatCodes: [PostgresFormatCode]
    var logger: Logger?

    init(_ query: String, as name: String) {
        self.query = query
        self.name = name
        self.resultFormatCodes = [.binary]
    }

    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        switch message.identifier {
        case .rowDescription:
            let row = try PostgresMessage.RowDescription(message: message)
            self.rowLookupTable = PostgresRow.LookupTable(
                rowDescription: row,
                resultFormat: self.resultFormatCodes
            )
            return []
        case .noData:
            return []
        case .parseComplete, .parameterDescription:
            return []
        case .readyForQuery:
            return nil
        default:
            fatalError("Unexpected message: \(message)")
        }

    }

    func start() throws -> [PostgresMessage] {
        let parse = PostgresMessage.Parse(
            statementName: self.name,
            query: self.query,
            parameterTypes: []
        )
        let describe = PostgresMessage.Describe(
            command: .statement,
            name: self.name
        )
        return try [parse.message(), describe.message(), PostgresMessage.Sync().message()]
    }


    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("\(self.query) prepared as \(self.name)")
    }
}


private final class ExecutePreparedQuery: PostgresRequest {
    let query: PreparedQuery
    let binds: [PostgresData]
    var onRow: (PostgresRow) throws -> ()
    var resultFormatCodes: [PostgresFormatCode]
    var logger: Logger?

    init(query: PreparedQuery, binds: [PostgresData], onRow: @escaping (PostgresRow) throws -> ()) {
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
            guard let rowLookupTable = query.rowLookupTable else {
                fatalError("row lookup was requested but never set")
            }
            let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
            try onRow(row)
            return []
        case .noData:
            return []
        case .commandComplete:
            return []
        case .readyForQuery:
            return nil
        default: throw PostgresError.protocol("Unexpected message during query: \(message)")
        }
    }

    func start() throws -> [PostgresMessage] {

        let bind = PostgresMessage.Bind(
            portalName: "",
            statementName: query.name,
            parameterFormatCodes: self.binds.map { $0.formatCode },
            parameters: self.binds.map { .init(value: $0.value) },
            resultFormatCodes: self.resultFormatCodes
        )
        let execute = PostgresMessage.Execute(
            portalName: "",
            maxRows: 0
        )

        let sync = PostgresMessage.Sync()
        return try [bind.message(), execute.message(), sync.message()]
    }

    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("Execute Prepared Query: \(query.name)")
    }

}
