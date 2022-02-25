import NIOCore
import struct Foundation.UUID

extension PostgresDatabase {
    public func prepare(query: String) -> EventLoopFuture<PreparedQuery> {
        let name = "nio-postgres-\(UUID().uuidString)"
        let request = PrepareQueryRequest(query, as: name)
        return self.send(PostgresCommands.prepareQuery(request: request), logger: self.logger).map { _ in
            // we can force unwrap the prepared here, since in a success case it must be set
            // in the send method of `PostgresDatabase`. We do this dirty trick to work around
            // the fact that the send method only returns an `EventLoopFuture<Void>`.
            // Eventually we should move away from the `PostgresDatabase.send` API.
            request.prepared!
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
    let underlying: PSQLPreparedStatement
    let database: PostgresDatabase

    init(underlying: PSQLPreparedStatement, database: PostgresDatabase) {
        self.underlying = underlying
        self.database = database
    }

    public func execute(_ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return self.execute(binds) { rows.append($0) }.map { rows }
    }

    public func execute(_ binds: [PostgresData] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let command = PostgresCommands.executePreparedStatement(query: self, binds: binds, onRow: onRow)
        return self.database.send(command, logger: self.database.logger)
    }

    public func deallocate() -> EventLoopFuture<Void> {
        self.underlying.connection.close(.preparedStatement(self.underlying.name), logger: self.database.logger)
    }
}

final class PrepareQueryRequest {
    let query: String
    let name: String
    var prepared: PreparedQuery? = nil
    
    
    init(_ query: String, as name: String) {
        self.query = query
        self.name = name
    }

}
