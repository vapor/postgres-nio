import NIO


/// PostgreSQL request to close a prepared statement or portal.
final class CloseRequest: PostgresRequest {

    /// Name of the prepared statement or portal to close.
    let name: String

    /// Close
    let target: PostgresMessage.Close.Target

    init(name: String, closeType: PostgresMessage.Close.Target) {
        self.name = name
        self.target = closeType
    }

    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if message.identifier != .closeComplete {
            fatalError("Unexpected PostgreSQL message \(message)")
        }
        return nil
    }

    func start() throws -> [PostgresMessage] {
        let close = try PostgresMessage.Close(target: target, name: name).message()
        let sync = try PostgresMessage.Sync().message()
        return [close, sync]
    }

    func log(to logger: Logger) {
        logger.debug("Requesting Close of \(name)")
    }
}
