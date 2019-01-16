import NIO

extension PostgresConnection {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return simpleQuery(string) { rows.append($0) }.map { rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let promise = self.channel.eventLoop.makePromise(of: Void.self)
        let handler = SimpleQueryHandler(query: string, onRow: onRow, promise: promise)
        return self.channel.pipeline.add(handler: handler).then {
            return promise.futureResult
        }
    }
    
    // MARK: Private
    
    private final class SimpleQueryHandler: PostgresConnectionHandler {
        var query: String
        var onRow: (PostgresRow) throws -> ()
        var promise: EventLoopPromise<Void>
        var error: Error?
        var rowLookupTable: PostgresRow.LookupTable?
        
        init(query: String, onRow: @escaping (PostgresRow) throws -> (), promise: EventLoopPromise<Void>) {
            self.query = query
            self.onRow = onRow
            self.promise = promise
        }
        
        func read(message: inout PostgresMessage, ctx: ChannelHandlerContext) throws {
            switch message.identifier {
            case .dataRow:
                let data = try PostgresMessage.DataRow.parse(from: &message)
                guard let rowLookupTable = self.rowLookupTable else { fatalError() }
                let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
                try onRow(row)
            case .rowDescription:
                let row = try PostgresMessage.RowDescription.parse(from: &message)
                self.rowLookupTable = PostgresRow.LookupTable(
                    rowDescription: row,
                    resultFormat: []
                )
            case .commandComplete: break
            case .error:
                let error = try PostgresMessage.Error.parse(from: &message)
                self.error = PostgresError(.server(error))
            case .notice:
                let notice = try PostgresMessage.Error.parse(from: &message)
                print("[NIOPostgres] [NOTICE] \(notice)")
            case .readyForQuery:
                ctx.pipeline.remove(handler: self, promise: nil)
            default: throw PostgresError(.protocol("Unexpected message during simple query: \(message)"))
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
            ctx.write(message: PostgresMessage.SimpleQuery(string: self.query), promise: nil)
            ctx.flush()
        }
    }
}
