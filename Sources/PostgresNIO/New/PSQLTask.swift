import Logging
import NIOCore

enum HandlerTask {
    case extendedQuery(ExtendedQueryContext)
    case closeCommand(CloseCommandContext)
    case startListening(NotificationListener)
    case cancelListening(String, Int)
}

enum PSQLTask {
    case extendedQuery(ExtendedQueryContext)
    case closeCommand(CloseCommandContext)

    func failWithError(_ error: PSQLError) {
        switch self {
        case .extendedQuery(let extendedQueryContext):
            switch extendedQueryContext.query {
            case .unnamed(_, let eventLoopPromise):
                eventLoopPromise.fail(error)
            case .executeStatement(_, let eventLoopPromise):
                eventLoopPromise.fail(error)
            case .prepareStatement(_, _, let eventLoopPromise):
                eventLoopPromise.fail(error)
            }

        case .closeCommand(let closeCommandContext):
            closeCommandContext.promise.fail(error)
        }
    }
}

final class ExtendedQueryContext {
    enum Query {
        case unnamed(PostgresQuery, EventLoopPromise<PSQLRowStream>)
        case executeStatement(PSQLExecuteStatement, EventLoopPromise<PSQLRowStream>)
        case prepareStatement(name: String, query: String, EventLoopPromise<RowDescription?>)
    }
    
    let query: Query
    let logger: Logger
    
    init(
        query: PostgresQuery,
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.query = .unnamed(query, promise)
        self.logger = logger
    }
    
    init(
        executeStatement: PSQLExecuteStatement,
        logger: Logger,
        promise: EventLoopPromise<PSQLRowStream>
    ) {
        self.query = .executeStatement(executeStatement, promise)
        self.logger = logger
    }

    init(
        name: String,
        query: String,
        logger: Logger,
        promise: EventLoopPromise<RowDescription?>
    ) {
        self.query = .prepareStatement(name: name, query: query, promise)
        self.logger = logger
    }
}

final class CloseCommandContext {
    let target: CloseTarget
    let logger: Logger
    let promise: EventLoopPromise<Void>
    
    init(target: CloseTarget,
         logger: Logger,
         promise: EventLoopPromise<Void>
    ) {
        self.target = target
        self.logger = logger
        self.promise = promise
    }
}

