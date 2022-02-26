#if swift(>=5.5.2) && canImport(_Concurrency)
extension PSQLConnection {

    public static func connect(
        configuration: PSQLConnection.Configuration,
        logger: Logger,
        on eventLoop: EventLoop,
        file: String = #file,
        line: UInt = #line
    ) async throws -> PSQLConnection {
        do {
            return try await Self.connect(configuration: configuration, logger: logger, on: eventLoop).get()
        } catch var error as PSQLError {
            logger.debug("connection creation failed", metadata: [
                .error: "\(error)"
            ])
            error.file = file
            error.line = line
            throw error
        }
    }
}
#endif
