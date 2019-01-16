import NIO

extension PostgresConnection {
    public func query(_ string: String, _ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: [PostgresData] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let handler = QueryHandler(query: string, binds: binds, onRow: onRow, promise: promise)
        return self.channel.pipeline.add(handler: handler).then {
            return promise.futureResult
        }
    }
    
    // MARK: Private
    
    private final class QueryHandler: PostgresConnectionHandler {
        let query: String
        let binds: [PostgresData]
        var onRow: (PostgresRow) throws -> ()
        var promise: EventLoopPromise<Void>
        var error: Error?
        var rowLookupTable: PostgresRow.LookupTable?
        var resultFormatCodes: [PostgresFormatCode]
        
        init(
            query: String,
            binds: [PostgresData],
            onRow: @escaping (PostgresRow) throws -> (),
            promise: EventLoopPromise<Void>
        ) {
            self.query = query
            self.binds = binds
            self.onRow = onRow
            self.promise = promise
            self.resultFormatCodes = [.binary]
        }
        
        func read(message: inout PostgresMessage, ctx: ChannelHandlerContext) throws {
            switch message.identifier {
            case .bindComplete: break
            case .dataRow:
                let data = try PostgresMessage.DataRow.parse(from: &message)
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
                do {
                    try onRow(row)
                } catch {
                    self.promise.fail(error: error)
                }
            case .rowDescription:
                let row = try PostgresMessage.RowDescription.parse(from: &message)
                self.rowLookupTable = PostgresRow.LookupTable(
                    rowDescription: row,
                    // tableNames: self.tableNames,
                    resultFormat: self.resultFormatCodes
                )
            case .noData: break
            case .parseComplete: break
            case .parameterDescription: break
            case .commandComplete: break
            case .error:
                let error = try PostgresMessage.Error.parse(from: &message)
                self.error = PostgresError(.server(error))
            case .notice:
                let notice = try PostgresMessage.Error.parse(from: &message)
                print("[NIOPostgres] [NOTICE] \(notice)")
            case .readyForQuery:
                ctx.pipeline.remove(handler: self, promise: nil)
            default: throw PostgresError(.protocol("Unexpected message during query: \(message)"))
            }
        }
        
        func errorCaught(ctx: ChannelHandlerContext, error: Error) {
            ctx.close(mode: .all, promise: nil)
            self.promise.fail(error: error)
        }
        
        func handlerRemoved(ctx: ChannelHandlerContext) {
            if let error = self.error {
                self.promise.fail(error: error)
            } else {
                self.promise.succeed(result: ())
            }
        }
        
        func handlerAdded(ctx: ChannelHandlerContext) {
            print("[NIOPostgres] \(self.query) \(self.binds)")
            
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
            
            let messages: [PostgresMessageType] = [parse, describe, bind, execute, sync]
            for message in messages {
                ctx.write(message: message, promise: nil)
            }
            ctx.flush()
        }
    }
}
