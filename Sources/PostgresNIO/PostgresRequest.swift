import Logging

public protocol PostgresRequest {
    // return nil to end request
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]?
    func start() throws -> [PostgresMessage]
    func log(to logger: Logger)
}
