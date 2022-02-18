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

    public func close() async throws {
        try await self.close().get()
    }

    public func query(_ query: PostgresQuery, logger: Logger, file: String = #file, line: UInt = #line) async throws -> PSQLRowSequence {
        var logger = logger
        logger[postgresMetadataKey: .connectionID] = "\(self.connectionID)"

        do {
            guard query.binds.count <= Int(Int16.max) else {
                throw PSQLError(.tooManyParameters)
            }
            let promise = self.channel.eventLoop.makePromise(of: PSQLRowStream.self)
            let context = ExtendedQueryContext(
                query: query,
                logger: logger,
                promise: promise)

            self.channel.write(PSQLTask.extendedQuery(context), promise: nil)

            return try await promise.futureResult.map({ $0.asyncSequence() }).get()
        } catch var error as PSQLError {
            logger.debug("query failed", metadata: [
                .error: "\(error)"
            ])
            error.file = file
            error.line = line
            throw error
        }
    }
}
#endif
