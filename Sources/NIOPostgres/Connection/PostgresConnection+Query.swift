import NIO

extension PostgresConnection {
    public func query(_ string: String, _ binds: PostgresBinds = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: PostgresBinds = [], _ onRow: @escaping (PostgresRow) -> ()) -> EventLoopFuture<Void> {
        let data: [PostgresData]
        do {
            data = try binds.serialize(allocator: self.handler.channel.allocator)
        } catch {
            return self.eventLoop.newFailedFuture(error: error)
        }
        let parse = PostgresMessage.Parse(
            statementName: "",
            query: string,
            parameterTypes: []
        )
        let describe = PostgresMessage.Describe(
            command: .statement,
            name: ""
        )
        let bind = PostgresMessage.Bind(
            portalName: "",
            statementName: "",
            parameterFormatCodes: data.map { $0.formatCode },
            parameters: data.map { .init(value: $0.value) },
            resultFormatCodes: [.binary]
        )
        let execute = PostgresMessage.Execute(
            portalName: "",
            maxRows: 0
        )
        var rowLookupTable: PostgresRow.LookupTable?
        return handler.send([
            .parse(parse), .describe(describe), .bind(bind), .execute(execute), .sync
        ]) { message in
            switch message {
            case .bindComplete:
                return false
            case .dataRow(let data):
                guard let rowLookupTable = rowLookupTable else { fatalError() }
                let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
                onRow(row)
                return false
            case .rowDescription(let r):
                rowLookupTable = PostgresRow.LookupTable(
                    rowDescription: r,
                    tableNames: self.tableNames,
                    resultFormat: bind.resultFormatCodes
                )
                return false
            case .parseComplete:
                return false
            case .parameterDescription(let desc):
                return false
            case .commandComplete(let complete):
                return false
            case .readyForQuery:
                return true
            default: throw PostgresError(.protocol("Unexpected message during query: \(message)"))
            }
        }
    }
}
