import Logging
import NIOCore

enum PSQLTask {
    case extendedQuery(ExtendedQueryContext)
    case preparedStatement(PrepareStatementContext)
    case closeCommand(CloseCommandContext)
    
    func failWithError(_ error: PSQLError) {
        switch self {
        case .extendedQuery(let extendedQueryContext):
            extendedQueryContext.promise.fail(error)
        case .preparedStatement(let createPreparedStatementContext):
            createPreparedStatementContext.promise.fail(error)
        case .closeCommand(let closeCommandContext):
            closeCommandContext.promise.fail(error)
        }
    }
}

final class ExtendedQueryContext {
    enum Query {
        case unnamed(PostgresQuery)
        case preparedStatement(PSQLExecuteStatement)
    }
    
    let query: Query
    let logger: Logger

    let promise: EventLoopPromise<PSQLRowStream>
    
    init(query: PostgresQuery,
         logger: Logger,
         promise: EventLoopPromise<PSQLRowStream>)
    {
        self.query = .unnamed(query)
        self.logger = logger
        self.promise = promise
    }
    
    init(executeStatement: PSQLExecuteStatement,
         logger: Logger,
         promise: EventLoopPromise<PSQLRowStream>)
    {
        self.query = .preparedStatement(executeStatement)
        self.logger = logger
        self.promise = promise
    }
}

final class PrepareStatementContext {
    let name: String
    let query: String
    let logger: Logger
    let promise: EventLoopPromise<RowDescription?>
    
    init(name: String,
         query: String,
         logger: Logger,
         promise: EventLoopPromise<RowDescription?>)
    {
        self.name = name
        self.query = query
        self.logger = logger
        self.promise = promise
    }
}

final class CloseCommandContext {
    let target: CloseTarget
    let logger: Logger
    let promise: EventLoopPromise<Void>
    
    init(target: CloseTarget,
         logger: Logger,
         promise: EventLoopPromise<Void>)
    {
        self.target = target
        self.logger = logger
        self.promise = promise
    }
}

