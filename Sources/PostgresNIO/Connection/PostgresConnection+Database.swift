import NIOCore
import Logging
import struct Foundation.Data

extension PostgresConnection: PostgresDatabase {
    public func send(
        _ request: PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void> {
        guard let command = request as? PostgresCommands else {
            preconditionFailure("\(#function) requires an instance of PostgresCommands. This will be a compile-time error in the future.")
        }
        
        let resultFuture: EventLoopFuture<Void>
        
        switch command {
        case .query(let query, let binds, let onMetadata, let onRow):
            resultFuture = self.underlying.query(query, binds, logger: logger).flatMap { stream in
                return stream.onRow(onRow).map { _ in
                    onMetadata(PostgresQueryMetadata(string: stream.commandTag)!)
                }
            }
        case .queryAll(let query, let binds, let onResult):
            resultFuture = self.underlying.query(query, binds, logger: logger).flatMap { rows in
                return rows.all().map { allrows in
                    onResult(.init(metadata: PostgresQueryMetadata(string: rows.commandTag)!, rows: allrows))
                }
            }
            
        case .prepareQuery(let request):
            resultFuture = self.underlying.prepareStatement(request.query, with: request.name, logger: self.logger).map {
                request.prepared = PreparedQuery(underlying: $0, database: self)
            }
        case .executePreparedStatement(let preparedQuery, let binds, let onRow):
            resultFuture = self.underlying.execute(preparedQuery.underlying, binds, logger: logger).flatMap { rows in
                return rows.onRow(onRow)
            }
        }
        
        return resultFuture.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }

    public func withConnection<T>(_ closure: (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

internal enum PostgresCommands: PostgresRequest {
    case query(query: String,
               binds: [PostgresData],
               onMetadata: (PostgresQueryMetadata) -> () = { _ in },
               onRow: (PostgresRow) throws -> ())
    case queryAll(query: String,
                  binds: [PostgresData],
                  onResult: (PostgresQueryResult) -> ())
    case prepareQuery(request: PrepareQueryRequest)
    case executePreparedStatement(query: PreparedQuery, binds: [PostgresData], onRow: (PostgresRow) throws -> ())
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        fatalError("This function must not be called")
    }
    
    func start() throws -> [PostgresMessage] {
        fatalError("This function must not be called")
    }
    
    func log(to logger: Logger) {
        fatalError("This function must not be called")
    }
}
