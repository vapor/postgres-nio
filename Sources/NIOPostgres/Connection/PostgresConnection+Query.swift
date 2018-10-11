import NIO

extension PostgresConnection {
    public func query(_ string: String, _ binds: PostgresBinds) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: PostgresBinds, _ onRow: @escaping (PostgresRow) -> ()) -> EventLoopFuture<Void> {
        var info: PostgresMessage.RowDescription?
        
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
            parameterFormatCodes: binds.data.map { $0.formatCode },
            parameters: binds.data.map { .init(data: $0.value) },
            resultFormatCodes: [.binary]
        )
        let execute = PostgresMessage.Execute(
            portalName: "",
            maxRows: 0
        )
        return handler.send([
            .parse(parse), .describe(describe), .bind(bind), .execute(execute), .sync
        ]) { message in
            switch message {
            case .bindComplete:
                return false
            case .dataRow(let data):
                guard let fields = info?.fields else { fatalError() }
                onRow(PostgresRow(fields: fields, columns: data.columns))
                return false
            case .rowDescription(let r):
                info = r
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
