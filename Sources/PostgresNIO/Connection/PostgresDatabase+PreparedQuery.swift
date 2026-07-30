import NIOCore
import NIOConcurrencyHelpers
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

    @preconcurrency
    public func prepare(query: String, handler: @Sendable @escaping (PreparedQuery) -> EventLoopFuture<[[PostgresRow]]>) -> EventLoopFuture<[[PostgresRow]]> {
        prepare(query: query)
        .flatMap { preparedQuery in
            handler(preparedQuery)
            .flatMap { results in
                preparedQuery.deallocate().map { results }
            }
        }
    }
}


public struct PreparedQuery: Sendable {
    let underlying: PSQLPreparedStatement
    let database: any PostgresDatabase

    init(underlying: PSQLPreparedStatement, database: any PostgresDatabase) {
        self.underlying = underlying
        self.database = database
    }

    public func execute(_ binds: [PostgresData] = []) -> EventLoopFuture<[PostgresRow]> {
        let rowsBoxed = NIOLockedValueBox([PostgresRow]())
        return self.execute(binds) { row in
            rowsBoxed.withLockedValue {
                $0.append(row)
            }
        }.map { rowsBoxed.withLockedValue { $0 } }
    }

    @preconcurrency
    public func execute(_ binds: [PostgresData] = [], _ onRow: @Sendable @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let command = PostgresCommands.executePreparedStatement(query: self, binds: binds, onRow: onRow)
        return self.database.send(command, logger: self.database.logger)
    }

    public func deallocate() -> EventLoopFuture<Void> {
        self.underlying.connection.close(.preparedStatement(self.underlying.name), logger: self.database.logger)
    }
}

final class PrepareQueryRequest: Sendable {
    let query: String
    let name: String
    var prepared: PreparedQuery? {
        get {
            self._prepared.withLockedValue { $0 }
        }
        set {
            self._prepared.withLockedValue {
                $0 = newValue
            }
        }
    }
    let _prepared: NIOLockedValueBox<PreparedQuery?> = .init(nil)

    init(_ query: String, as name: String) {
        self.query = query
        self.name = name
    }
}
