import Logging

/// Protocol to encapsulate a function call on the Postgres server
///
/// This protocol is deprecated going forward. 
public protocol PostgresRequest {
    // return nil to end request
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]?
    func start() throws -> [PostgresMessage]
    func log(to logger: Logger)
}
